<?php

declare(strict_types=1);

namespace Queuer\Messaging;

use Aws\Sqs\Exception\SqsException;
use Aws\Sqs\SqsClient;
use Psr\Log\LoggerInterface;
use Zend\Config\Config;

final class SqsProducer implements ProducerInterface
{
    /** @var SqsClient */
    protected $client;

    /** @var Config */
    protected $config;

    /** @var LoggerInterface  */
    protected $logger;

    protected $newQueueAttributes = [
        'DelaySeconds' => 5,
        'MaximumMessageSize' => 262144
    ];

    private $queueUrl;

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
     * @inheritdoc
     * @param string $topic
     * @param mixed $message
     */
    public function publish(string $topic, $message): void
    {
        try {
            if (!$this->queueExists($topic)) {
                $this->createQueue($topic);
            }

            $this->client->sendMessage([
                'QueueUrl' => $this->queueUrl,
                'MessageBody' => is_array($message) ? json_encode($message) : $message
            ]);

        } catch (\Exception $e) {
            throw new ProducerException($e->getMessage(), $e->getCode());
        }
    }

    /**
     * @param string $topic
     * @throws SqsException
     */
    private function createQueue(string $topic): void
    {
        $fire = $this->client->createQueue([
            'QueueName' => $topic,
            'Attributes' => $this->newQueueAttributes
        ]);

        $queueUrl = $fire->get('QueueUrl');

        if (!empty($queueUrl)) {
            $this->queueUrl = $queueUrl;
            $this->createDeadQueue($queueUrl, $topic);
        }
    }

    /**
     * @param string $queueUrl
     * @param string $topic
     * @throws SqsException
     */
    private function createDeadQueue($queueUrl, $topic)
    {
        $fire = $this->client->createQueue([
            'QueueName' => sprintf('dead-queue-%s', $topic),
            'Attributes' => $this->newQueueAttributes
        ]);
        $deadQueueUrl = $fire->get('QueueUrl');
        if (!empty($deadQueueUrl)) {
            $arn = $this->client->getQueueArn($deadQueueUrl);
            $this->client->setQueueAttributes([
                'Attributes' => [
                    'RedrivePolicy' => "{\"deadLetterTargetArn\":\"" . $arn . "\",\"maxReceiveCount\":\"10\"}"
                ],
                'QueueUrl' => $queueUrl
            ]);
        }
    }

    private function getQueueUrl(string $topic)
    {
        try {
            $result = $this->client->getQueueUrl([
                'QueueName' => $topic
            ]);

            $url = $result->get('QueueUrl');
            $this->queueUrl = $url;
            return $url;
        } catch (SqsException $e) {
            return null;
        }
    }

    /**
     * @param string $topic
     * @return bool
     */
    protected function queueExists(string $topic): bool
    {
        return (bool) $this->getQueueUrl($topic);
    }
}
