<?php

declare(strict_types=1);

namespace Queuer\Messaging;

interface ConsumerInterface
{
    /**
     * Consome mensagens da fila. Deve ser implementado como um processo long-running
     *
     * @param string $topic
     * @param callable $handler
     *
     * @throws ConsumerException
     */
    public function subscribe(string $topic, callable $handler): void;

    /**
     * @param string $topic
     * @param int $maxMessages
     * @return array
     */
    public function pooling(string $topic, int $maxMessages = 10): array;

    /**
     * Deve retornar uma lista de topicos que podem ser consumidos
     *
     * @param string|null $prefix
     * @return array
     */
    public function listTopics(string $prefix = null): array;

    /**
     * Remover mensagem do topico
     *
     * @param string $topic
     * @param string $messageId
     */
    public function removeMessageFromTopicAndId(string $topic, string $messageId): void;

    /**
     * Retorna o nome do topico (fila) reduzido, usado nas configurações
     * @param string $topic
     * @return string
     */
    public function getShortTopic(string $topic): string;
}
