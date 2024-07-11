<?php

namespace Pocket\Djob;

use Exception;
use stdClass;
use Workerman\Worker;

interface DjobHandler
{
    /**
     * Handle the job payload
     *
     * @param mixed $payload Job payload
     * @param stdClass $job Current job object
     * @param Worker $jobWorker Job consumption process Worker instance
     * @return mixed
     * @throws Exception Please throw an exception to indicate job failure
     */
    public function handle($payload, $job, Worker $jobWorker);
}