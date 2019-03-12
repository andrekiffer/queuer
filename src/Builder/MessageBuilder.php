<?php

namespace Queuer\Builder;

use Queuer\Message;
use Queuer\Messaging\ProducerInterface;

final class MessageBuilder
{
    private $producer;
    private $topic;
    private $source;
    private $parameters;
    private $command;
    private $message;

    public function __construct(ProducerInterface $producer, string $source)
    {
        $this->producer = $producer;
        $this->source = $source;
    }

    public function setTopic(string $topic)
    {
        $this->topic = $topic;
        return $this;
    }

    public function setCommand(string $command)
    {
        $this->command = $command;
        return $this;
    }

    public function setParameters(string $parameters)
    {
        $this->parameters = $parameters;
        return $this;
    }

    public function setMessage(array $message)
    {
        $this->message = $message;
        return $this;
    }

    public function send(): void
    {
        $message = new Message(
            $this->command,
            $this->message,
            (string) $this->parameters,
            $this->source
        );

        $this->producer->publish($this->topic, $message->toArray());
    }
}