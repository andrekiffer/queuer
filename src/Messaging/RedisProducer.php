<?php

declare(strict_types=1);

namespace Queuer\Messaging;

use Predis\Client;

final class RedisProducer implements ProducerInterface
{
    /** @var Client */
    private $redisClient;

    public function __construct(Client $redisClient)
    {
        $this->redisClient = $redisClient;
    }

    public function publish(string $topic, $message): void
    {
        try {
            $stringMessage = is_array($message) ? json_encode($message) : $message;
            $this->redisClient->publish(
                $topic,
                $stringMessage
            );
        } catch (\Exception $e) {
            throw new ProducerException($e->getMessage(), $e->getCode());
        }
    }
}
