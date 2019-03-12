<?php

namespace Queuer\Application\Factory;

use Queuer\Application\QueueManager;
use Zend\Config\Config;

class QueuerManagerFactory
{
    public static function create(array $configuration) : QueueManager
    {
        $queueManagerConfig = new Config([
            'prefix-topic' => $configuration['prefix-topic'],
            'prefix-exclude-topic' => $configuration['prefix-exclude']
        ]);

        $consumer = ConsumerFactory::create(
            $configuration['type'],
            [$configuration['type'] => $configuration['config']]
        );

        return new QueueManager(
            $consumer,
            $queueManagerConfig
        );
    }
}
