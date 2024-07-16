<?php

namespace Procket\Djob;

use Closure;
use DateTime;
use ErrorException;
use Exception;
use InvalidArgumentException;
use Laravel\SerializableClosure\SerializableClosure;
use PDO;
use PDOException;
use RuntimeException;
use stdClass;
use Workerman\Worker;

class Djob
{
    /**
     * Instance cache array
     *
     * @var Djob[]
     */
    private static $instances = [];

    /**
     * Database configuration
     *
     * @var array
     */
    private $dbConfig;

    /**
     * Database connection instance
     *
     * @var PDO
     */
    private $connection;

    /**
     * Table name of jobs
     *
     * @var string
     */
    private $jobsTable;

    /**
     * Whether the table of jobs exists
     *
     * @var bool
     */
    private $jobsTableExists = false;

    /**
     * Table name of failed jobs
     *
     * @var string
     */
    private $failedJobsTable;

    /**
     * Whether the table of failed jobs exists
     *
     * @var bool
     */
    private $failedJobsTableExists = false;

    /**
     * Whether to store successfully executed jobs
     *
     * @var bool
     */
    private $saveSuccessfulJobs;

    /**
     * Table name of successful jobs
     *
     * @var string|null
     */
    private $successfulJobsTable;

    /**
     * Whether the table of successful jobs exists
     *
     * @var bool
     */
    private $successfulJobsTableExists = false;

    /**
     * The last dispatched job id
     *
     * @var int|null
     */
    private $lastDispatchedJobId = null;

    /**
     * The last dispatched job exception
     *
     * @var Exception|null
     */
    private $lastDispatchedJobException = null;

    /**
     * Create Djob instance
     *
     * The default configuration of the database connection is as follows:
     *
     * ```
     * [
     *      'dsn' => 'mysql:host=localhost;port=3306;dbname=database;charset=utf8mb4',
     *      'username' => 'username',
     *      'password' => 'password',
     *      'options' => [
     *          PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci'
     *      ],
     *      'prefix' => ''
     * ]
     * ```
     *
     * @param array|null $dbConfig Database connection configuration array
     * @param string $jobsTable Table name for jobs
     * @param bool $saveSuccessfulJobs Whether to store successfully executed jobs
     */
    public function __construct(array $dbConfig = null, $jobsTable = 'djobs', $saveSuccessfulJobs = false)
    {
        $this->dbConfig = array_replace([
            'dsn' => 'mysql:host=localhost;port=3306;dbname=database;charset=utf8mb4',
            'username' => 'username',
            'password' => 'password',
            'options' => [
                PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci'
            ],
            'prefix' => ''
        ], (array)$dbConfig);
        if (
            $this->dbConfig['prefix'] &&
            strncmp($jobsTable, $this->dbConfig['prefix'], strlen($this->dbConfig['prefix'])) === 0
        ) {
            $jobsTable = substr($jobsTable, strlen($this->dbConfig['prefix']));
        }
        $this->jobsTable = $this->dbConfig['prefix'] . $jobsTable;
        $this->saveSuccessfulJobs = $saveSuccessfulJobs;
        $this->failedJobsTable = $this->dbConfig['prefix'] . 'failed_' . $jobsTable;
        $this->successfulJobsTable = $this->dbConfig['prefix'] . 'successful_' . $jobsTable;

        $this->connect();
        $this->ensureTablesExist();
    }

    /**
     * Ensure all the necessary job tables exist
     *
     * @return void
     */
    public function ensureTablesExist()
    {
        $this->ensureJobsTableExists();
        $this->ensureFailedJobsTableExists();
        if ($this->saveSuccessfulJobs) {
            $this->ensureSuccessfulJobsTableExists();
        }
    }

    /**
     * Get Djob instance
     *
     * This method will return the same instance for the same arguments.
     *
     * @param array|null $dbConfig Database connection configuration array
     * @param string $jobsTable Table name for jobs
     * @param bool $saveSuccessfulJobs Whether to store successfully executed jobs
     * @return static
     */
    public static function instance(array $dbConfig = null, $jobsTable = 'djobs', $saveSuccessfulJobs = false)
    {
        $cacheKey = md5(serialize(func_get_args()));

        if (isset(self::$instances[$cacheKey])) {
            return self::$instances[$cacheKey];
        }

        $instance = new static((array)$dbConfig, $jobsTable, $saveSuccessfulJobs);

        return self::$instances[$cacheKey] = $instance;
    }

    /**
     * Connect to the database
     *
     * @return PDO
     */
    protected function connect()
    {
        $this->connection = new PDO(
            $this->dbConfig['dsn'],
            $this->dbConfig['username'],
            $this->dbConfig['password'],
            $this->dbConfig['options']
        );
        $this->connection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        // fix Mysql<=5.6.17 PDO Warning: Packets out of order.
        $this->connection->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);

        return $this->connection;
    }

    /**
     * Create job related table
     *
     * @param string $table Table name
     * @return void
     */
    protected function buildTable($table)
    {
        $tableExists = (bool)$this->getConnection()->query("SHOW TABLES LIKE '{$table}'")->fetchObject();

        if (!$tableExists) {
            if (false !== stripos($table, 'failed_') || false !== stripos($table, 'successful_')) {
                $autoIncr = "";
            } else {
                $autoIncr = "AUTO_INCREMENT";
            }

            $buildQuery = "
                CREATE TABLE IF NOT EXISTS `{$table}` (
                    `id` bigint(20) unsigned NOT NULL {$autoIncr},
                    `mark` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
                    `wait` text COLLATE utf8mb4_unicode_ci NOT NULL,
                    `priority` int(11) NOT NULL DEFAULT '0',
                    `handler` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
                    `payload` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
                    `attempts` tinyint(3) unsigned NOT NULL DEFAULT '0',
                    `max_attempts` tinyint(3) unsigned NOT NULL DEFAULT '1',
                    `available_at` int(10) unsigned NOT NULL DEFAULT '0',
                    `created_at` int(10) unsigned NOT NULL,
                    `result` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
                    `is_locked` tinyint(1) unsigned NOT NULL DEFAULT '0',
                    PRIMARY KEY (`id`),
                    KEY `mark` (`mark`) USING BTREE,
                    KEY `available_at` (`available_at`) USING BTREE,
                    KEY `created_at` (`created_at`) USING BTREE,
                    KEY `handler-priority-id` (`handler`, `priority`, `id`) USING BTREE,
                    KEY `priority-id` (`priority`, `id`) USING BTREE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ";
            $this->getConnection()->exec($buildQuery);
        }
    }

    /**
     * Delete job related table
     *
     * @param string $table Table name
     * @return void
     */
    protected function dropTable($table)
    {
        $dropQuery = "DROP TABLE IF EXISTS {$table}";

        $this->getConnection()->exec($dropQuery);
    }

    /**
     * Get database connection instance
     *
     * @return PDO
     */
    protected function getConnection()
    {
        try {
            if ($this->connection instanceof PDO) {
                $connected = (bool)$this->connection->getAttribute(PDO::ATTR_SERVER_INFO);
            } else {
                $connected = false;
            }
        } catch (PDOException $e) {
            $connected = false;
        }

        // Reconnect to database
        if (!$connected) {
            $this->connect();
        }

        return $this->connection;
    }

    /**
     * Get table name of jobs
     *
     * @return string
     */
    protected function getJobsTable()
    {
        return $this->jobsTable;
    }

    /**
     * Ensure the table of jobs exists
     *
     * @return void
     */
    protected function ensureJobsTableExists()
    {
        if (!$this->jobsTableExists) {
            $this->buildJobsTable();
        }

        $this->jobsTableExists = true;
    }

    /**
     * Create table of jobs
     *
     * @return void
     */
    protected function buildJobsTable()
    {
        $this->buildTable($this->getJobsTable());
    }

    /**
     * Delete table of jobs
     *
     * @return void
     */
    protected function dropJobsTable()
    {
        $this->dropTable($this->getJobsTable());
    }

    /**
     * Get table name of failed jobs
     *
     * @return string
     */
    protected function getFailedJobsTable()
    {
        return $this->failedJobsTable;
    }

    /**
     * Ensure the table of failed jobs exists
     *
     * @return void
     */
    protected function ensureFailedJobsTableExists()
    {
        if (!$this->failedJobsTableExists) {
            $this->buildFailedJobsTable();
        }

        $this->failedJobsTableExists = true;
    }

    /**
     * Create table of failed jobs
     *
     * @return void
     */
    protected function buildFailedJobsTable()
    {
        $this->buildTable($this->getFailedJobsTable());
    }

    /**
     * Delete table of failed jobs
     *
     * @return void
     */
    protected function dropFailedJobsTable()
    {
        $this->dropTable($this->getFailedJobsTable());
    }

    /**
     * Get table name of successful jobs
     *
     * @return string
     */
    protected function getSuccessfulJobsTable()
    {
        return $this->successfulJobsTable;
    }

    /**
     * Ensure the table of successful jobs exists
     *
     * @return void
     */
    protected function ensureSuccessfulJobsTableExists()
    {
        if (!$this->successfulJobsTableExists) {
            $this->buildSuccessfulJobsTable();
        }

        $this->successfulJobsTableExists = true;
    }

    /**
     * Create table of successful jobs
     *
     * @return void
     */
    protected function buildSuccessfulJobsTable()
    {
        $this->buildTable($this->getSuccessfulJobsTable());
    }

    /**
     * Delete table of successful jobs
     *
     * @return void
     */
    protected function dropSuccessfulJobsTable()
    {
        $this->dropTable($this->getSuccessfulJobsTable());
    }

    /**
     * Retry operation
     *
     * @param int|array $times Number of retries
     * @param callable $callback Operation callback
     * @param int|Closure $sleepMilliseconds Sleep milliseconds
     * @param callable|null $when Retry only when this callback function returns true
     * @return mixed
     *
     * @throws Exception
     */
    protected function retry($times, callable $callback, $sleepMilliseconds = 0, $when = null)
    {
        $attempts = 0;

        $backoff = [];

        if (is_array($times)) {
            $backoff = $times;

            $times = count($times) + 1;
        }

        beginning:
        $attempts++;
        $times--;

        try {
            return $callback($attempts);
        } catch (Exception $e) {
            if ($times < 1 || ($when && !$when($e))) {
                throw $e;
            }

            $sleepMilliseconds = isset($backoff[$attempts - 1]) ? $backoff[$attempts - 1] : $sleepMilliseconds;

            if ($sleepMilliseconds) {
                usleep($sleepMilliseconds * 1000);
            }

            goto beginning;
        }
    }

    /**
     * Ensure the directory exists
     *
     * @param string $path Directory path
     * @param int $mode Directory permissions
     * @param bool $recursive Whether to create recursively
     * @return bool
     */
    protected function ensureDirectory($path, $mode = 0755, $recursive = true)
    {
        if (is_dir($path)) {
            return true;
        }

        return @mkdir($path, $mode, $recursive);
    }

    /**
     * Parse date string
     *
     * String supports format accepted by {@link https://secure.php.net/manual/en/function.date.php date()},
     * the default date format placeholder is the percent sign (%).
     * ```
     * For example: 'error_%Y%m%d.log' will be parsed as 'error_20230907.log'
     * ```
     *
     * @param string $string String to be parsed
     * @param string $placeholder Date format placeholder
     * @return string
     */
    protected function parseDateString($string, $placeholder = '%')
    {
        if (false === strpos($string, $placeholder)) {
            return $string;
        }

        /** @var string[] $dateChars */
        $dateChars = str_split('dDjlNSwzWFmMntLoXxYyaABgGhHisuveIOPpTZcrU');
        $now = new DateTime();

        $replacePairs = [];
        foreach ($dateChars as $dateChar) {
            $escapedChar = $placeholder . $dateChar;
            $dateStr = $now->format($dateChar);
            $replacePairs[$escapedChar] = $dateStr;
        }

        return strtr($string, $replacePairs);
    }

    /**
     * Determine whether the exception is caused by a concurrency error such as a deadlock or serialization failure
     *
     * @param Exception $e
     * @return bool
     */
    protected function causedByConcurrencyError(Exception $e)
    {
        if ($e instanceof PDOException && ($e->getCode() === 40001 || $e->getCode() === '40001')) {
            return true;
        }

        $message = $e->getMessage();

        $errorMessages = [
            'Deadlock found when trying to get lock',
            'deadlock detected',
            'The database file is locked',
            'database is locked',
            'database table is locked',
            'A table in the database is locked',
            'has been chosen as the deadlock victim',
            'Lock wait timeout exceeded; try restarting transaction',
            'WSREP detected deadlock/conflict and aborted the transaction. Try restarting the transaction',
            'There is already an active transaction',
            'There is no active transaction',
        ];

        foreach ($errorMessages as $errorMessage) {
            if (false !== stripos($message, $errorMessage)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Determine whether the exception is caused by a lost connection
     *
     * @param Exception $e
     * @return bool
     */
    protected function causedByLostConnection(Exception $e)
    {
        $message = $e->getMessage();

        $errorMessages = [
            'server has gone away',
            'no connection to the server',
            'Lost connection',
            'is dead or not enabled',
            'Error while sending',
            'decryption failed or bad record mac',
            'server closed the connection unexpectedly',
            'SSL connection has been closed unexpectedly',
            'Error writing data to the connection',
            'Resource deadlock avoided',
            'Transaction() on null',
            'child connection forced to terminate due to client_idle_limit',
            'query_wait_timeout',
            'reset by peer',
            'Physical connection is not usable',
            'TCP Provider: Error code 0x68',
            'ORA-03114',
            'Packets out of order. Expected',
            'Adaptive Server connection failed',
            'Communication link failure',
            'connection is no longer usable',
            'Login timeout expired',
            'SQLSTATE[HY000] [2002] Connection refused',
            'running with the --read-only option so it cannot execute this statement',
            'The connection is broken and recovery is not possible. The connection is marked by the client driver as unrecoverable. No attempt was made to restore the connection.',
            'SQLSTATE[HY000] [2002] php_network_getaddresses: getaddrinfo failed: Try again',
            'SQLSTATE[HY000] [2002] php_network_getaddresses: getaddrinfo failed: Name or service not known',
            'SQLSTATE[HY000]: General error: 7 SSL SYSCALL error: EOF detected',
            'SQLSTATE[HY000] [2002] Connection timed out',
            'SSL: Connection timed out',
            'SQLSTATE[HY000]: General error: 1105 The last transaction was aborted due to Seamless Scaling. Please retry.',
            'Temporary failure in name resolution',
            'SSL: Broken pipe',
            'SQLSTATE[08S01]: Communication link failure',
            'SQLSTATE[08006] [7] could not connect to server: Connection refused Is the server running on host',
            'SQLSTATE[HY000]: General error: 7 SSL SYSCALL error: No route to host',
            'The client was disconnected by the server because of inactivity. See wait_timeout and interactive_timeout for configuring this behavior.',
            'SQLSTATE[08006] [7] could not translate host name',
            'TCP Provider: Error code 0x274C'
        ];

        foreach ($errorMessages as $errorMessage) {
            if (false !== stripos($message, $errorMessage)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Execute closure method in transaction
     *
     * @param Closure $callback
     * @return mixed
     *
     * @throws Exception
     */
    protected function transaction(Closure $callback)
    {
        $this->getConnection()->beginTransaction();

        try {
            $callbackResult = $callback($this);
            $this->getConnection()->commit();
        } catch (Exception $e) {
            $this->getConnection()->rollBack();
            throw $e;
        }

        return $callbackResult;
    }

    /**
     * Fetch a pending job
     *
     * @param string|null $handler The handler class name that implements {@see DjobHandler}, such as SendMailHandler::class.
     * @param int|null $id Job ID
     * @param int|null $offsetId Job ID offset (internal call, generally do not pass this parameter).
     * @return stdClass|null
     */
    protected function fetchPendingJob($handler = null, $id = null, $offsetId = null)
    {
        $jobQueryBoundValues = [];
        $jobQuery = "SELECT * FROM {$this->getJobsTable()} WHERE";
        $jobQuery .= " `is_locked` = 0";
        $jobQuery .= " AND `attempts` < `max_attempts`";
        $jobQuery .= " AND `available_at` < " . time();
        if (!is_null($handler)) {
            $jobQuery .= " AND `handler` = ?";
            $jobQueryBoundValues[] = $handler;
        }
        if (!is_null($id)) {
            $jobQuery .= " AND `id` = ?";
            $jobQueryBoundValues[] = $id;
        }
        if ($offsetId > 0) {
            $jobQuery .= " AND `id` > ?";
            $jobQueryBoundValues[] = $offsetId;
        }
        $jobQuery .= " ORDER BY `priority` ASC, `id` ASC";
        $jobQuery .= " LIMIT 1";

        $statement = $this->getConnection()->prepare($jobQuery);
        $statement->execute($jobQueryBoundValues);
        $job = $statement->fetchObject();
        if (!$job) {
            return null;
        }

        if ($job->wait && ($waitOptions = unserialize($job->wait)) && is_array($waitOptions)) {
            $waitAny = $waitOptions['any'];
            $waitType = $waitOptions['type'];
            $waitValues = $waitOptions['value'];
            $isPending = false;
            switch ($waitType) {
                case 'id':
                    $isPending = $this->checkPendingById($waitValues, !$waitAny);
                    break;
                case 'mark':
                case 'mark_prefix':
                    $isPending = $this->checkPendingByMark($waitValues, !$waitAny, $waitType === 'mark_prefix');
                    break;
            }
            if ($isPending) {
                // fetch the next pending job
                return $this->fetchPendingJob($handler, $id, $job->id);
            }
        }

        return $job;
    }

    /**
     * Lock job
     *
     * @param stdClass $job Job object
     * @return bool
     */
    protected function lockJob($job)
    {
        if (!is_object($job) || !isset($job->id)) {
            return false;
        }

        $updateLockQuery = "UPDATE {$this->getJobsTable()} SET `is_locked` = 1 WHERE `is_locked` = 0 AND `id` = {$job->id}";

        return $this->getConnection()->exec($updateLockQuery) > 0;
    }

    /**
     * Unlock job
     *
     * @param stdClass $job Job object
     * @return bool
     */
    protected function unlockJob($job)
    {
        if (!is_object($job) || !isset($job->id)) {
            return false;
        }

        $updateLockQuery = "UPDATE {$this->getJobsTable()} SET `is_locked` = 0 WHERE `is_locked` = 1 AND `id` = {$job->id}";

        return $this->getConnection()->exec($updateLockQuery) > 0;
    }

    /**
     * Refresh job
     *
     * @param stdClass $job Job object
     * @return stdClass|null
     */
    protected function refreshJob($job)
    {
        if (!is_object($job) || !isset($job->id)) {
            return null;
        }

        $jobQuery = "SELECT * FROM {$this->getJobsTable()} WHERE `id` = ? LIMIT 1";
        $statement = $this->getConnection()->prepare($jobQuery);
        $statement->execute([$job->id]);
        $job = $statement->fetchObject();

        return $job ?: null;
    }

    /**
     * Save successful job
     *
     * @param stdClass $job Job object
     * @return bool
     */
    protected function saveSuccessfulJob($job)
    {
        if (!$this->saveSuccessfulJobs) {
            return true;
        }

        if (!is_object($job) || !isset($job->id)) {
            return false;
        }
        if (!($job = $this->refreshJob($job))) {
            return false;
        }

        $insertSuccessfulJobQuery = "
            INSERT INTO `{$this->getSuccessfulJobsTable()}`
                (`id`, `mark`, `wait`, `priority`, `handler`, `payload`, `attempts`, `max_attempts`, `available_at`, `created_at`, `result`, `is_locked`)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        ";
        $statement = $this->getConnection()->prepare($insertSuccessfulJobQuery);

        return $statement->execute([
            $job->id,
            $job->mark,
            $job->wait,
            $job->priority,
            $job->handler,
            $job->payload,
            $job->attempts,
            $job->max_attempts,
            $job->available_at,
            $job->created_at,
            $job->result,
            $job->is_locked
        ]);
    }

    /**
     * Save failed job
     *
     * @param stdClass $job Job object
     * @param Exception|null $jobException Job exception
     * @return bool
     */
    protected function saveFailedJob($job, $jobException = null)
    {
        if (!is_object($job) || !isset($job->id)) {
            return false;
        }
        if (!($job = $this->refreshJob($job))) {
            return false;
        }

        $insertFailedJobQuery = "
            INSERT INTO `{$this->getFailedJobsTable()}`
                (`id`, `mark`, `wait`, `priority`, `handler`, `payload`, `attempts`, `max_attempts`, `available_at`, `created_at`, `result`, `is_locked`)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        ";
        $statement = $this->getConnection()->prepare($insertFailedJobQuery);

        return $statement->execute([
            $job->id,
            $job->mark,
            $job->wait,
            $job->priority,
            $job->handler,
            $job->payload,
            $job->attempts,
            $job->max_attempts,
            $job->available_at,
            $job->created_at,
            (string)$jobException,
            $job->is_locked
        ]);
    }

    /**
     * Delete job
     *
     * @param stdClass $job Job object
     * @return bool
     */
    protected function deleteJob($job)
    {
        if (!is_object($job) || !isset($job->id)) {
            return false;
        }

        $deleteQuery = "DELETE FROM {$this->getJobsTable()} WHERE `id` = ? LIMIT 1";
        $statement = $this->getConnection()->prepare($deleteQuery);

        return $statement->execute([$job->id]);
    }

    /**
     * Error handling method
     *
     * @param int $level Exception level
     * @param string $message Exception message
     * @param string|null $file The filename where the exception is thrown.
     * @param int|null $line The line number where the exception is thrown.
     * @return bool
     * @throws ErrorException
     */
    protected function handleError($level, $message, $file = null, $line = null)
    {
        if (!(error_reporting() & $level)) {
            return false;
        }
        throw new ErrorException($message, $level, $level, $file, $line);
    }

    /**
     * Check if the exception level is a fatal error
     *
     * @param int $level Exception level
     * @return bool
     */
    protected function isFatalError($level)
    {
        $fatalErr = E_ERROR | E_USER_ERROR | E_PARSE | E_CORE_ERROR | E_CORE_WARNING | E_COMPILE_ERROR |
            E_COMPILE_WARNING | E_RECOVERABLE_ERROR;

        return ($level & $fatalErr) > 0;
    }

    /**
     * Check if on Windows system
     *
     * @return bool
     */
    public function isOnWindows()
    {
        return stripos(PHP_OS, 'WIN') === 0;
    }

    /**
     * Push payload to the job handler
     *
     * ```
     * The default options are as follows:
     * [
     *      // Job mark
     *      'mark' => '',
     *      // Delay seconds, future timestamp or date string.
     *      'delay' => 0,
     *      // Max number of attempts
     *      'max_attempts' => 1,
     *      // Processing priority, the larger the number, the higher the priority, recommend 0 to 9.
     *      'priority' => 0,
     *      // Wait options
     *      'wait' => '',
     * ]
     *
     * Wait options are as follows：
     * [
     *      // If the wait value is an array, it will be executed when any job in the array is completed.
     *      'any' => false,
     *      // Wait type, empty means no waiting, supports 'id', 'mark', 'mark_prefix'.
     *      'type' => '',
     *      // Wait value, please pass an array if multiple.
     *      'value' => ''
     * ]
     * ```
     *
     * @param mixed|Closure $payload Job payload
     * @param string|null $handler Job handler
     * @param array $options Job options
     * @return int Returns the created job ID
     * @throws Exception
     */
    public function push($payload, $handler = null, array $options = [])
    {
        if ($payload instanceof Closure) {
            $payload = new SerializableClosure($payload);
            $handler = isset($handler) ? $handler : ClosureHandler::class;
        }
        if (!is_subclass_of($handler, DjobHandler::class)) {
            throw new RuntimeException("Handler [{$handler}] must be a subclass of [" . DjobHandler::class . "]");
        }

        $options = array_replace([
            'mark' => '',
            'delay' => 0,
            'max_attempts' => 1,
            'priority' => 0,
            'wait' => '',
        ], $options);
        $waitOptions = !$options['wait'] || !is_array($options['wait']) ? [] : $options['wait'];
        $supportedWaitTypes = ['id', 'mark', 'mark_prefix'];

        $now = time();
        $availableAt = $now;
        $waitAny = isset($waitOptions['any']) && $waitOptions['any'];
        $waitType = isset($waitOptions['type']) ? trim($waitOptions['type']) : '';
        $waitValues = array_filter((array)(isset($waitOptions['value']) ? $waitOptions['value'] : ''));
        if ($waitType) {
            if (!in_array($waitType, $supportedWaitTypes, true)) {
                throw new InvalidArgumentException(sprintf(
                    "Wait type [%s] is not supported, please pass in one of %s",
                    $waitType,
                    json_encode($supportedWaitTypes)
                ));
            }
            if (!$waitValues) {
                throw new InvalidArgumentException("Wait value cannot be empty");
            }
            $wait = serialize(array_replace($waitOptions, [
                'any' => $waitAny,
                'type' => $waitType,
                'value' => $waitValues
            ]));
        } else {
            $wait = '';
        }

        if (is_int($options['delay']) || ctype_digit($options['delay'])) {
            if ($options['delay'] > $now) {
                $availableAt = $options['delay'];
            } else {
                $availableAt = $now + $options['delay'];
            }
        } else if (is_string($options['delay']) && strtotime($options['delay']) > 0) {
            $availableAt = strtotime($options['delay']);
        }

        $insertQuery = "
            INSERT INTO `{$this->getJobsTable()}`
                (`mark`, `wait`, `priority`, `handler`, `payload`, `attempts`, `max_attempts`, `available_at`, `created_at`, `result`, `is_locked`)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        ";
        $statement = $this->getConnection()->prepare($insertQuery);
        $statement->execute([
            (string)$options['mark'],
            $wait,
            $options['priority'] * -1,
            $handler,
            serialize($payload),
            0,
            $options['max_attempts'],
            $availableAt,
            $now,
            '',
            0
        ]);
        $jobId = (int)$this->getConnection()->lastInsertId();

        if ($jobId < 1) {
            throw new RuntimeException("Failed to push payload to the [{$handler}] handler");
        }

        return $jobId;
    }

    /**
     * Get the last dispatched job id
     *
     * @return int|null
     */
    public function getLastDispatchedJobId()
    {
        return $this->lastDispatchedJobId;
    }

    /**
     * Get the last dispatched job exception
     *
     * @return Exception|null
     */
    public function getLastDispatchedJobException()
    {
        return $this->lastDispatchedJobException;
    }

    /**
     * Dispatch a job
     *
     * @param Worker $jobWorker Job consumption process Worker instance
     * @param string|null $handler Job handler class name, e.g. SendMailHandler::class.
     * @param int|null $id Job ID
     * @return void
     * @throws Exception
     */
    public function dispatch(Worker $jobWorker, $handler = null, $id = null)
    {
        try {
            $job = $this->fetchPendingJob($handler, $id);
            if (!$this->lockJob($job)) {
                return;
            }
            $payload = null;
            $handlerInstance = null;
        } catch (Exception $e) {
            return;
        }

        try {
            set_error_handler(function ($errLevel, $errMsg, $errFile, $errLine) {
                $this->handleError($errLevel, $errMsg, $errFile, $errLine);
            });

            if (is_object($job)) {
                $jobQuery = "SELECT * FROM {$this->getJobsTable()} WHERE `is_locked` = 1 AND `id` = ? FOR UPDATE";
                $statement = $this->getConnection()->prepare($jobQuery);
                $statement->execute([$job->id]);
                $job = $statement->fetchObject() ?: null;
            }
            if (!is_object($job)) {
                $this->lastDispatchedJobId = null;
                $this->lastDispatchedJobException = null;
                return;
            }
            $payload = unserialize($job->payload);

            if (!class_exists($job->handler)) {
                throw new ErrorException("Job handler class [{$job->handler}] not found");
            }
            /** @var DjobHandler $handlerInstance */
            $handlerInstance = new $job->handler;
            if (method_exists($handlerInstance, 'onDispatching')) {
                $handlerInstance->onDispatching($payload, $job, $jobWorker);
            }

            $this->retry($job->max_attempts, function () use ($handlerInstance, $payload, $job, $jobWorker) {
                $result = null;
                try {
                    $result = $handlerInstance->handle($payload, $job, $jobWorker);
                } finally {
                    $this->lastDispatchedJobId = $job->id;
                    $incrAttemptsQuery = "
                        UPDATE {$this->getJobsTable()} SET `attempts` = `attempts` + 1, `result` = ?
                        WHERE `id` = {$job->id}
                    ";
                    $statement = $this->getConnection()->prepare($incrAttemptsQuery);
                    $statement->execute([serialize($result)]);
                }
            });

            $this->saveSuccessfulJob($job);
        } catch (Exception $e) {
            $this->lastDispatchedJobException = $e;
            if (isset($job)) {
                $this->saveFailedJob($job, $e);
            }
        } finally {
            if (isset($job)) {
                $this->unlockJob($job);
                $this->deleteJob($job);
            }

            restore_error_handler();
        }

        if ($this->lastDispatchedJobException) {
            if (isset($handlerInstance) && method_exists($handlerInstance, 'onException')) {
                $handlerInstance->onException($this->lastDispatchedJobException, $payload, $job, $jobWorker);
            }
            throw $this->lastDispatchedJobException;
        } else {
            if (isset($handlerInstance) && method_exists($handlerInstance, 'onDispatched') && is_object($job)) {
                $handlerInstance->onDispatched($payload, $job, $jobWorker);
            }
        }
    }

    /**
     * Dispatch a job by handler
     *
     * @param Worker $jobWorker Job consumption process Worker instance
     * @param string $handler Job handler class name, e.g. SendMailHandler::class.
     * @return void
     * @throws Exception
     */
    public function dispatchHandler(Worker $jobWorker, $handler)
    {
        $this->dispatch($jobWorker, $handler);
    }

    /**
     * Dispatch a job by ID
     *
     * @param Worker $jobWorker Job consumption process Worker instance
     * @param int $id Job ID
     * @return void
     * @throws Exception
     */
    public function dispatchId(Worker $jobWorker, $id)
    {
        $this->dispatch($jobWorker, null, $id);
    }

    /**
     * Check if job is pending by job ID
     *
     * @param int|array $id Job ID
     * @param bool $any If Job ID is an array, whether to check any ID in the array is pending.
     * @return bool
     */
    public function checkPendingById($id, $any = false)
    {
        $ids = (array)$id;
        $placeholders = array_fill(0, count($ids), '?');
        $placeholdersSql = implode(', ', $placeholders);
        $jobQuery = "SELECT `id` FROM {$this->getJobsTable()} WHERE `id` IN ({$placeholdersSql})";
        $statement = $this->getConnection()->prepare($jobQuery);
        $statement->execute($ids);
        $pendingJobIds = $statement->fetchAll(PDO::FETCH_COLUMN) ?: [];

        foreach ($ids as $checkId) {
            if ($any) {
                if (in_array($checkId, $pendingJobIds)) {
                    return true;
                }
            } else {
                if (!in_array($checkId, $pendingJobIds)) {
                    return false;
                }
            }
        }

        return !$any;
    }

    /**
     * Check if job has been dispatched by job ID
     *
     * @param int|array $id Job ID
     * @param bool $any If Job ID is an array, whether to check any ID in the array has been dispatched.
     * @return bool
     */
    public function checkDispatchedById($id, $any = false)
    {
        return !$this->checkPendingById($id, !$any);
    }

    /**
     * Check if job is pending by job mark
     *
     * @param string|array $mark Job mark
     * @param bool $any If Job mark is an array, whether to check any mark in the array is pending.
     * @param bool $isPrefix Whether the mark is a prefix, defaults to false which means full character matching.
     * @return bool
     */
    public function checkPendingByMark($mark, $any = false, $isPrefix = false)
    {
        $marks = (array)$mark;
        if ($isPrefix) {
            $markValues = [];
            foreach ($marks as $markPrefix) {
                $markValues[] = "{$markPrefix}%";
            }
            $conditions = array_fill(0, count($marks), '`mark` LIKE ?');
            $conditionsSql = implode(' OR ', $conditions);
            $jobQuery = "SELECT `mark` FROM {$this->getJobsTable()} WHERE {$conditionsSql} GROUP BY `mark`";
            $statement = $this->getConnection()->prepare($jobQuery);
            $statement->execute($markValues);
        } else {
            $placeholders = implode(', ', array_fill(0, count($marks), '?'));
            $jobQuery = "SELECT `mark` FROM {$this->getJobsTable()} WHERE `mark` IN ({$placeholders}) GROUP BY `mark`";
            $statement = $this->getConnection()->prepare($jobQuery);
            $statement->execute($marks);
        }
        $pendingJobMarks = $statement->fetchAll(PDO::FETCH_COLUMN) ?: [];

        foreach ($marks as $checkMark) {
            if ($isPrefix) {
                $isPending = false;
                foreach ($pendingJobMarks as $pendingJobMark) {
                    if ($checkMark && strncasecmp($pendingJobMark, $checkMark, strlen($checkMark)) === 0) {
                        $isPending = true;
                        break;
                    }
                }
            } else {
                $isPending = in_array($checkMark, $pendingJobMarks, true);
            }

            if ($any) {
                if ($isPending) {
                    return true;
                }
            } else {
                if (!$isPending) {
                    return false;
                }
            }
        }

        return !$any;
    }

    /**
     * Check if job has been dispatched by job mark
     *
     * @param string|array $mark Job mark
     * @param bool $any If Job mark is an array, whether to check any mark in the array has been dispatched.
     * @param bool $isPrefix Whether the mark is a prefix, defaults to false which means full character matching.
     * @return bool
     */
    public function checkDispatchedByMark($mark, $any = false, $isPrefix = false)
    {
        return !$this->checkPendingByMark($mark, !$any, $isPrefix);
    }

    /**
     * Start consumption process
     *
     * Because Windows does not support launching child processes,
     * the ***processNumber*** parameter is invalid on Windows systems.
     *
     * @param string $name Consumption process name
     * @param int $processNumber Number of child processes
     * @param int|null $loopNumber How many times each child process loops at most, null means infinite.
     * @param string|null $handler Job handler class name, e.g. SendMailHandler::class.
     * @param string|null $errorLogFile Job error log file path, supports date format characters, see {@see Djob::parseDateString()}.
     * @return void
     */
    public function start($name = 'djob', $processNumber = 1, $loopNumber = 500, $handler = null, $errorLogFile = null)
    {
        if (!is_null($errorLogFile) && !$this->ensureDirectory($dir = dirname($errorLogFile))) {
            throw new RuntimeException("Failed to create the log directory [{$dir}]");
        }

        $this->registerWorker($name, $processNumber, $loopNumber, $handler, $errorLogFile);

        if (!defined('START_ALL_WORKERS')) {
            Worker::runAll();
        }
    }

    /**
     * Register job consumption process
     *
     * @param string $name Consumption process name
     * @param int $processNumber Number of child processes
     * @param int|null $loopNumber How many times each child process loops at most, null means infinite.
     * @param string|null $handler Job handler class name, e.g. SendMailHandler::class.
     * @param string|null $errorLogFile Job error log file path, supports date format characters, see {@see Djob::parseDateString()}.
     * @return void
     */
    protected function registerWorker($name = 'djob', $processNumber = 1, $loopNumber = 500, $handler = null, $errorLogFile = null)
    {
        /** @noinspection PhpObjectFieldsAreOnlyWrittenInspection */
        $jobWorker = new Worker();
        $jobWorker->name = $name;
        $jobWorker->count = $processNumber;

        $jobWorker->onWorkerStart = function (Worker $jobWorker) use ($loopNumber, $handler, $errorLogFile) {
            // Create a new instance with the same configuration to re-create the database connection
            $djob = new static($this->dbConfig, $this->jobsTable, $this->saveSuccessfulJobs);
            while (is_null($loopNumber) || (--$loopNumber >= 0)) {
                try {
                    $djob->dispatch($jobWorker, $handler);
                } catch (Exception $e) {
                    $isConcurrencyError = $djob->causedByConcurrencyError($e);
                    $isLostConnectionError = $djob->causedByLostConnection($e);
                    if ($isConcurrencyError || $isLostConnectionError) {
                        if ($errorLogFile) {
                            $logMsg = sprintf("[%s] %s\n", date('Y-m-d H:i:s'), $e);
                            file_put_contents($errorLogFile, $logMsg, FILE_APPEND);
                        }
                        usleep(200 * 1000);
                        $djob->connect();
                        continue;
                    }
                    $errMsg = "Job ID [{$djob->getLastDispatchedJobId()}] error: {$e->getMessage()}";
                    echo $errMsg . PHP_EOL;
                    if ($errorLogFile) {
                        $logMsg = sprintf(
                            "[%s] [%s] %s\n",
                            date('Y-m-d H:i:s'),
                            $djob->getLastDispatchedJobId(),
                            $e
                        );
                        file_put_contents($djob->parseDateString($errorLogFile), $logMsg, FILE_APPEND);
                    }
                } finally {
                    if (is_null($djob->getLastDispatchedJobId())) {
                        // Sleep 1 second if the job queue is empty
                        usleep(1000 * 1000);
                    }
                    if ($loopNumber === 0) {
                        Worker::stopAll();
                    }
                }
            }
        };
    }
}