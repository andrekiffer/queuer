<?php

namespace Queuer\Application;

use Queuer\Messaging\ConsumerInterface;
use Zend\Config\Config;

class QueueManager implements \Countable
{
    /** @var ConsumerInterface */
    private $consumer;
    /** @var array */
    private $config;
    /** @var array */
    private $topics;

    public function __construct(
        ConsumerInterface $consumer,
        Config $config
    ) {
        $this->consumer = $consumer;
        $this->config = $config;
    }

    public function getConsumer()
    {
        return $this->consumer;
    }

    public function updateConfiguration(Config $config)
    {
        $this->config = $config;
        $this->refreshTopics();
    }

    public function topics()
    {
        return $this->topics;
    }

    public function count()
    {
        return count($this->topics());
    }

    /**
     * Update list of topics
     */
    public function refreshTopics()
    {
        $this->topics = $this
            ->consumer
            ->listTopics($this->config->get('prefix-topic'));

        $this->topics = array_values(array_filter(
            $this->topics,
            function ($topic) {
                $prefixRemoveTopic = $this->config->get('prefix-exclude-topic');

                if (!$prefixRemoveTopic) {
                    return true;
                }

                return strpos($topic, $prefixRemoveTopic) === false;
            }
        ));
    }

    public function getCommandsFromTopic(string $topic)
    {
        $commands = [];
        foreach ($this->consumer->pooling($topic) as $message) {
            array_push($commands, $message);
        }

        return $commands;
    }

    public function removeMessageFromQueue(string $topic, $messageId)
    {
        $this->consumer->removeMessageFromTopicAndId($topic, $messageId);
    }

    public function getFirstCommandFromTopic($topic)
    {
        return $this->consumer->pooling($topic, 1);
    }

    public function getCommands()
    {
        $commands = [];
        foreach ($this->topics() as $topic) {
            foreach ($this->getCommandsFromTopic($topic) as $command) {
                array_push($commands, $command);
            }
        }

        return $commands;
    }

    public static function keepOnQueue(string $message, array $context = [])
    {
        echo $message . ($context ? ' | Context: ' . json_encode($context) : '') . PHP_EOL;
        exit(1);
    }
}
