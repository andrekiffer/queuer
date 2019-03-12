<?php

chdir(__DIR__);

require '../vendor/autoload.php';

$producer = \Queuer\Application\Factory\ProducerFactory::create('sqs', [
    'sqs' => [
        'version' => 'latest',
        'region' => 'sa-east-1',
        'key' => getenv('SQS_KEY'),
        'secret' => getenv('SQS_SECRET')
    ]
]);

$messageBuilder = new \Queuer\Builder\MessageBuilder($producer, 'queuer');
$messageBuilder
    ->setParameters('--sleep=5')
    ->setTopic('queuer-test')
    ->setCommand('test')
    ->setMessage([
        'teste' => 'a\'ha"hah"'
    ])
    ->send();
