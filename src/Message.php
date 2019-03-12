<?php

namespace Queuer;

use Queuer\Exception\InvalidQueuerMessageException;

final class Message
{
    private $version = 'v1';
    private $source;
    private $command;
    private $payload;
    private $occurredAt;

    public function __construct(
        string $command,
        array $payload,
        string $parameters = '',
        string $source = null
    ) {
        $this->command = $command;
        $this->payload = $payload;
        $this->occurredAt = new \DateTime;
        $this->source = $source;
        $this->parameters = $parameters;
    }

    public function __toString()
    {
        if (!$this->isValid()) {
            throw new InvalidQueuerMessageException('Invalid message format');
        }

        return json_encode($this->toArray());
    }

    private function isValid(): bool
    {
        $message = json_encode($this->toArray());
        return (bool)json_decode($message, true);
    }

    public function toArray()
    {
        return [
            'version' => $this->version,
            'source' => $this->source,
            'command' => $this->command,
            'parameters' => $this->parameters,
            'occurred_at' => $this->occurredAt->format('c'),
            'payload' => $this->payload
        ];
    }
}
