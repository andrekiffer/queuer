<?php

namespace Queuer\Messaging;

use Psr\Log\LoggerInterface;

class RetryDecoratorConsumer implements ConsumerInterface
{
    const MAX_RETRIES = 3;

    /**
     * @var ConsumerInterface
     */
    private $next;

    /**
     * @var LoggerInterface
     */
    private $logger;

    public function __construct(
        ConsumerInterface $next,
        LoggerInterface $logger
    ) {
        $this->next = $next;
        $this->logger = $logger;
    }

    private function stdoutLog($message, array $context)
    {
        echo json_encode(array_merge(['time' => date('c'), 'message' => $message], $context)) . PHP_EOL;
    }

    public function listTopics(string $prefix = null): array
    {
        return $this->next->listTopics($prefix);
    }

    public function pooling(string $topic, int $maxMessages = 10): array
    {
        return $this->next->pooling($topic, $maxMessages);
    }

    public function removeMessageFromTopicAndId(string $topic, string $messageId): void
    {
        $this->next->removeMessageFromTopicAndId($topic, $messageId);
    }

    public function subscribe(string $topic, callable $handler): void
    {
        $this
            ->next
            ->subscribe(
                $topic,
                function ($data) use ($handler) {
                    $done = false;
                    $attempts = 0;
                    /** @var \Throwable $lastException */
                    $lastException = null;

                    while (!$done && self::MAX_RETRIES > $attempts) {
                        try {
                            $handler($data);
                            $done = true;
                        } catch (\Exception $e) {
                            $attempts++;
                            $this->stdoutLog('Retrying', [
                                'attempt' => $attempts,
                                'exception' => $e->getMessage(),
                                'exception_class' => get_class($e)
                            ]);
                            $lastException = $e;
                        }
                    }

                    if ($done === false && $attempts >= self::MAX_RETRIES) {
                        $this->logger->error('Error sqs: ' . $lastException->getMessage(), [
                            'payload' => json_encode($data)
                        ]);
                        throw $lastException;
                    }
                });
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