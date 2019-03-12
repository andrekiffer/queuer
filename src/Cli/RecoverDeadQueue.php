<?php

namespace Queuer\Cli;

use Queuer\Application\Factory\ConsumerFactory;
use Queuer\Application\Factory\ProducerFactory;
use Queuer\Messaging\ConsumerInterface;
use Queuer\Messaging\ProducerInterface;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Yaml\Yaml;


class RecoverDeadQueue extends Command
{
    protected function configure()
    {
        $this
            ->setName('recover-dead-queue')
            ->addOption(
                'config',
                null,
                InputOption::VALUE_REQUIRED,
                'Configuração por meio de arquivo'
            )
            ->addOption(
                'queue',
                null,
                InputOption::VALUE_REQUIRED,
                'Sistema de fila'
            )
            ->setDescription('Recover dead queue and run it again');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $queue = $input->getOption('queue');

        $configurationFile = $input->getOption('config');
        $config = $this->yamlParser($configurationFile);

        if ($config) {
            $queueConfig['sqs'] = $config['queue']['config'];
        }

        /** @var ConsumerInterface $sqsConsumer */
        $sqsConsumer = ConsumerFactory::createSqs($queueConfig);
        /** @var ProducerInterface $sqsProducer */
        $sqsProducer = ProducerFactory::createSqs($queueConfig);
        $topics      = $sqsConsumer->listTopics('dead-queue-' . $queue);

        foreach ($topics as $topic) {
            $fullTopicUri = $topic;
            $topic        = explode('/', $topic);
            $topic        = end($topic);

            while (true) {
                $messages = $sqsConsumer->pooling($fullTopicUri);

                if (empty($messages)) {
                    break;
                }

                foreach ($messages as $message) {
                    $messageId        = $message['message_id'];
                    $data             = $message['data'];
                    $deadQueueTopic   = $topic;
                    $destinationTopic = str_replace('dead-queue-', '', $topic);
                    echo $deadQueueTopic . ' -> ' . $destinationTopic . ' message: ' . json_encode($data) . PHP_EOL;
                    $sqsProducer->publish($destinationTopic, $data);
                    $sqsConsumer->removeMessageFromTopicAndId($fullTopicUri, $messageId);
                }
            }
        }
    }

    private function yamlParser($ymlPath)
    {
        if (!$ymlPath) {
            return null;
        }

        $yamlContent = file_get_contents($ymlPath);
        return Yaml::parse($yamlContent);
    }
}
