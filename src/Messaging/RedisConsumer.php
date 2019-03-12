<?php

declare(strict_types=1);

namespace Queuer\Messaging;

use Predis\Client;
use Psr\Log\LoggerInterface;
use Queuer\Process\Daemon;

final class RedisConsumer implements ConsumerInterface
{
    /** @var Client */
    private $client;
    /** @var LoggerInterface */
    private $logger;

    public function __construct(Client $client, LoggerInterface $logger)
    {
        $this->client = $client;
        $this->logger = $logger;
    }

    public function pooling(string $topic, int $maxMessages = 10): array
    {
        throw new \BadMethodCallException('Método não implementado');
    }

    public function listTopics(string $prefix = null): array
    {
        throw new \BadMethodCallException('Método não implementado');
    }

    public function removeMessageFromTopicAndId(string $topic, string $messageId): void
    {
        throw new \BadMethodCallException('Método não implementado');
    }

    public function subscribe(string $topic, callable $handler): void
    {
        Daemon::run(function () use ($topic, $handler) {
            try {
                $pubsub = $this->client->pubSubLoop();
                $pubsub->subscribe($topic);

                /** @var \stdClass $message */
                foreach ($pubsub as $message) {
                    switch ($message->kind) {
                        case 'message':
                            if ($message->channel !== $topic) {
                                continue;
                            }
                            $this->logStart($topic, $message);
                            $handler($message->payload);
                            $this->logFinish($topic, $message);
                            break;
                    }
                }
            } catch (\Throwable $e) {
                echo $e->getMessage() . PHP_EOL;
            }
            sleep(1);
        }, true);
    }

    private function logStart($topic, $message)
    {
        $message = $message->payload;
        $this
            ->logger
            ->info(sprintf('Message from redis pub/sub:  %s', trim(substr($message, 0, 200))), [
                'messaging' => [
                    'topic' => $topic,
                    'worker' => 'redis'
                ]
            ]);
    }

    private function logFinish($topic, $message)
    {
        $message = $message->payload;
        $this
            ->logger
            ->info(sprintf('Success handle message from redis pub/sub:  %s...', trim(substr($message, 0, 200))), [
                'messaging' => [
                    'topic' => $topic,
                    'worker' => 'redis'
                ]
            ]);
    }

    /**
     * Retorna o nome do topico (fila) reduzido, usado nas configurações
     * @param string $topic
     * @return string
     */
    public function getShortTopic(string $topic): string
    {
        return $topic;
    }
}
