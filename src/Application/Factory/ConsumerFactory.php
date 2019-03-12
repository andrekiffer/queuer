<?php

namespace Queuer\Application\Factory;

use Aws\Sqs\SqsClient;
use Predis\Client;
use Queuer\Messaging\RedisConsumer;
use Queuer\Messaging\RetryDecoratorConsumer;
use Queuer\Messaging\SqsConsumer;
use Psr\Log\NullLogger;
use Zend\Config\Config;

class ConsumerFactory
{
    public static function create(string $queue, array $parameters)
    {
        switch ($queue) {
            case 'sqs':
                return self::createSqs($parameters);
            case 'redis':
                return self::createRedis($parameters);
            default:
                throw new \LogicException('Factory not implemented');
        }
    }

    public static function createSqs(array $parameters)
    {
        /** @var \Aws\Sqs\SqsClient $sqsClient */
        $sqsClient = SqsClient::factory([
            'credentials' => [
                'key' => $parameters['sqs']['key'],
                'secret' => $parameters['sqs']['secret'],
            ],
            'version' => $parameters['sqs']['version'],
            'region' => $parameters['sqs']['region']
        ]);
        $config = new Config($parameters);
        $logger = LoggerFactory::create();

        return new SqsConsumer(
            $sqsClient,
            $config,
            $logger
        );
    }

    public static function createRedis(array $parameters)
    {
        $client = new Client($parameters['redis']);
        $logger = LoggerFactory::create();

        return new RedisConsumer(
            $client,
            $logger
        );
    }
}
