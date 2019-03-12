<?php

namespace Queuer\Application\Command;

use Queuer\Exception\InvalidQueuerMessageException;

class ExecuteTask
{
    /** @var string */
    private $messageId;
    /** @var string */
    private $commandId;
    /** @var array */
    private $payload;
    /** @var string */
    private $parameters;

    public function __construct(
        string $messageId,
        string $commandId,
        array $payload,
        string $parameters = ''
    ) {
        $this->messageId = $messageId;
        $this->commandId = $commandId;
        $this->parameters = $parameters;
        $this->payload = $payload;
    }

    public static function from(array $message)
    {
        if (!isset($message['message_id'])) {
            throw new InvalidQueuerMessageException('Please set message_id');
        }

        if (!isset($message['command'])) {
            throw new InvalidQueuerMessageException('Message without "command" attribute');
        }

        if (!isset($message['payload'])) {
            throw new InvalidQueuerMessageException('Message without "payload" attribute');
        }

        return new self(
            $message['message_id'],
            $message['command'],
            $message['payload'],
            (string) $message['parameters']
        );
    }

    public function messageId(): string
    {
        return $this->messageId;
    }

    public function commandId(): string
    {
        return $this->commandId;
    }

    public function payload(): array
    {
        return $this->payload;
    }

    public function parameters(): string
    {
        return $this->parameters;
    }

    public function getExecutionCommand(string $appConsole)
    {
        $payload  = $this->payload();
        $payload = \json_encode($payload);
        $payload = str_replace("'", "'\"'\"'", $payload);

        return sprintf(
            "%s %s %s --payload='%s'",
            $appConsole,
            $this->commandId(),
            $this->parameters(),
            $payload
        );
    }
}
