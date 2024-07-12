<?php

use Procket\Djob\DjobHandler;
use Workerman\Worker;

class TestHandler implements DjobHandler
{
    /**
     * @inheritDoc
     */
    public function handle($payload, $job, Worker $jobWorker)
    {
        echo $payload . PHP_EOL;
    }
}