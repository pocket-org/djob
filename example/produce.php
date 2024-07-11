<?php

use Pocket\Djob\Djob;

require __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/TestHandler.php';

$djob = Djob::instance(include __DIR__ . '/connection.php');

for ($i = 1; $i <= 10; $i++) {
    // use handler
    $djob->push($i, TestHandler::class);
    // use closure
    $djob->push(function () use ($i) {
        echo $i * 100 . PHP_EOL;
    });
}