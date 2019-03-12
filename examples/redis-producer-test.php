<?php

chdir(__DIR__);

require '../vendor/autoload.php';

$producer = \Queuer\Application\Factory\ProducerFactory::create('redis', [
    'redis' => [
        'host' => 'localhost',
        'port' => 6379
    ]
]);

$producer->publish('test', 'aa');

