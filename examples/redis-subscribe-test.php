<?php

chdir(__DIR__);

require '../vendor/autoload.php';

$consumer = \Queuer\Application\Factory\ConsumerFactory::create('redis', [
    'redis' => [
        'host' => 'localhost',
        'port' => 6379
    ]
]);

$consumer->subscribe('test', function($data) {
    var_dump($data);
});

