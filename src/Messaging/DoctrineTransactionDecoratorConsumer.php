<?php

namespace Queuer\Messaging;

use Doctrine\ORM\EntityManagerInterface;
use Psr\Log\LoggerInterface;

class DoctrineTransactionDecoratorConsumer implements ConsumerInterface
{
    /**
     * @var EntityManagerInterface
     */
    private $entityManager;

    /**
     * @var ConsumerInterface
     */
    private $next;

    /**
     * @var LoggerInterface
     */
    private $logger;

    public function __construct(
        EntityManagerInterface $entityManager,
        ConsumerInterface $next,
        LoggerInterface $logger
    ) {
        $this->entityManager = $entityManager;
        $this->next = $next;
        $this->logger = $logger;
    }

    private function stdoutLog($message, array $context = [])
    {
        echo json_encode(array_merge(['time' => date('c'), 'message' => $message], $context)) . PHP_EOL;
    }

    public function subscribe(string $topic, callable $handler): void
    {
        $entityManager = $this->entityManager;
        $this
            ->next
            ->subscribe(
                $topic,
                function ($data) use ($handler, $entityManager) {
                    try {
                        $this->stdoutLog('Open Transaction');
                        $entityManager->getConnection()->beginTransaction();
                        $handler($data);
                        $entityManager->getConnection()->commit();
                        $this->stdoutLog('Commit Transaction');
                    } catch (\Exception $e) {
                        $entityManager->getConnection()->rollback();
                        $this->stdoutLog('Rollback Transaction');
                        throw $e;
                    }
                }
            );
    }

    /**
     * @param string $topic
     * @param int $maxMessages
     * @return array
     */
    public function pooling(string $topic, int $maxMessages = 10): array
    {
        // TODO: Implement pooling() method.
    }

    /**
     * Deve retornar uma lista de topicos que podem ser consumidos
     *
     * @param string|null $prefix
     * @return array
     */
    public function listTopics(string $prefix = null): array
    {
        // TODO: Implement listTopics() method.
    }

    /**
     * Remover mensagem do topico
     *
     * @param string $topic
     * @param string $messageId
     */
    public function removeMessageFromTopicAndId(string $topic, string $messageId): void
    {
        // TODO: Implement removeMessageFromTopicAndId() method.
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
