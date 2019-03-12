<?php

declare(strict_types=1);

namespace Queuer\Messaging;

interface ProducerInterface
{
    /**
     * Envia uma mensagem para a fila
     *
     * @param string $topic
     * @param mixed $message
     *
     * @throws ProducerException
     */
    public function publish(string $topic, $message) : void;
}