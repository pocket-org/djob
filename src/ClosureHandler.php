<?php

namespace Pocket\Djob;

use Laravel\SerializableClosure\SerializableClosure;
use Workerman\Worker;

class ClosureHandler implements DjobHandler
{
    /**
     * @inheritDoc
     */
    public function handle($payload, $job, Worker $jobWorker)
    {
        /** @var SerializableClosure $serializableClosure */
        $serializableClosure = $payload;
        $closure = $serializableClosure->getClosure();
        $closure();
    }
}