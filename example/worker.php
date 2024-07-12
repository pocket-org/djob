<?php

use Procket\Djob\Djob;

require __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/TestHandler.php';

$djob = Djob::instance(include __DIR__ . '/connection.php');

if ($djob->isOnWindows()) {
    // Windows for testing purposes only
    $djob->start(1, null);
} else {
    $djob->start(4);
}