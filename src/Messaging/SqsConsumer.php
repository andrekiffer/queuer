<?php

declare(strict_types=1);

namespace Queuer\Messaging;

use Aws\Sqs\SqsClient;
use Psr\Log\LoggerInterface;
use Queuer\Process\Daemon;
use Zend\Config\Config;

final class SqsConsumer implements ConsumerInterface
{
    /**
     * Tempo em segundos para long-polling
     */
    const WAIT_TIME_SECONDS = 20;

    /**
     * @var SqsClient
     */
    protected $client;

    /**
     * @var Config
     */
    protected $config;

    /**
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * SqsConsumer constructor.
     * @param SqsClient $client
     * @param Config $config
     * @param LoggerInterface $logger
     */
    public function __construct(
        SqsClient $client,
        Config $config,
        LoggerInterface $logger
    ) {
        $this->client = $client;
        $this->config = $config;
        $this->logger = $logger;
    }

    /**
     * Subscribe a handler to a topic.
     *
     * @param string $topic
     * @param callable $handler
     *
     * @throws ConsumerException
     */
    public function subscribe(string $topic, callable $handler): void
    {
        Daemon::run(function () use ($topic, $handler) {
            $res = $this->receiveMessage($topic);

            if (empty($res->get('Messages'))) {
                $this->logger->info(sprintf('Without messages %s', $topic));
                return;
            }

            foreach ($res->getPath('Messages') as $message) {
                $this->logger->info(sprintf('Received Msg:  %s', trim(substr($message['Body'], 0, 200))), [
                    'messaging' => [
                        'topic' => $topic
                    ]
                ]);

                $handler($message['Body']);

                $this->logger->info(sprintf('Success:  %s...', trim(substr($message['Body'], 0, 200))), [
                    'messaging' => [
                        'topic' => $topic
                    ]
                ]);

                $this->commit($topic, $message);
            }
        });
    }

    public function pooling(string $topic, int $maxMessages = 10): array
    {
        $messages = [];
        $res = $this->receiveMessage($topic, 0, $maxMessages);

        if (empty($res->get('Messages'))) {
            return [];
        }

        foreach ($res->get('Messages') as $message) {
            $data = json_decode($message['Body'], true);
            if (!$data) {
                continue;
            }
            array_push($messages, [
                'message_id' => $message['ReceiptHandle'],
                'data' => $data
            ]);
        }

        return $messages;
    }

    public function removeMessageFromTopicAndId(string $topic, string $messageId): void
    {
        $this->commit($topic, [
            'ReceiptHandle' => $messageId
        ]);
    }

    public function listTopics(string $prefix = null): array
    {
        $queues = $this->client->listQueues([
            'QueueNamePrefix' => $prefix
        ]);
        $response = $queues->toArray();

        if (!isset($response['QueueUrls'])) {
            return [];
        }

        return $response['QueueUrls'];
    }

    protected function receiveMessage(
        string $topic,
        int $waitTimeSeconds = self::WAIT_TIME_SECONDS,
        int $maxNumberOfMessages = 10
    ) {
        $fulltopic = $this->config->get('sqs')->get('host')
            ? $this->config->get('sqs')->get('host') . $topic
            : $topic;

        $message = $this->client->receiveMessage([
            'QueueUrl' => $fulltopic,
            'WaitTimeSeconds' => $waitTimeSeconds,
            'MaxNumberOfMessages' => $maxNumberOfMessages
        ]);

        return $message;
    }

    protected function commit(string $topic, array $message): void
    {
        if (!filter_var($topic, FILTER_VALIDATE_URL)) {
            $topic = $this->config->get('sqs')->get('host')
                ? $this->config->get('sqs')->get('host') . $topic
                : $topic;
        }
        $this->client->deleteMessage(array(
            'QueueUrl' => $topic,
            'ReceiptHandle' => $message['ReceiptHandle']
        ));

        $this->logger->info('Remove message from topic', ['topic' => $topic]);
    }

    /**
     * Retorna o nome do topico (fila) reduzido, usado nas configurações
     * @param string $topic
     * @return string
     */
    public function getShortTopic(string $topic): string
    {
        $url = parse_url($topic);
        $parts = explode('/', $url['path']);
        return end($parts);
    }
}
