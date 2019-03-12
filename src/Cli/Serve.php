<?php

namespace Queuer\Cli;

use Queuer\Application\Factory\ConsumerFactory;
use Queuer\Application\Factory\LoggerFactory;
use Queuer\Application\QueueManager;
use Queuer\Application\Supervisor;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Yaml\Exception\ParseException;
use Symfony\Component\Yaml\Yaml;
use Zend\Config\Config;

class Serve extends Command
{
    protected function configure()
    {
        $this
            ->setName('serve')
            ->addOption(
                'config',
                null,
                InputOption::VALUE_OPTIONAL,
                'Configuração por meio de arquivo'
            )
            ->addOption(
                'app-console',
                null,
                InputOption::VALUE_REQUIRED,
                'Console application que o Queuer irá chamar para executar o commando da fila'
            )
            ->addOption(
                'prefix-topic',
                null,
                InputOption::VALUE_OPTIONAL,
                'Prefixo dos tópicos que o Queuer irá fazer pooling'
            )
            ->addOption(
                'prefix-exclude-topic',
                null,
                InputOption::VALUE_OPTIONAL,
                'Prefixo para ignorar topicos. Esses tópicos não serão assinados pelo Queuer'
            )
            ->addOption(
                'topic-list-refresh',
                null,
                InputOption::VALUE_OPTIONAL,
                'Intervalo em segundos para atualizar a lista de tópicos da fila'
            )
            ->addOption(
                'queue',
                null,
                InputOption::VALUE_REQUIRED,
                'Sistema de fila'
            )
            ->addOption(
                'queue-config',
                null,
                InputOption::VALUE_REQUIRED,
                'Json de configuração dos parâmetros para conexão na fila'
            )
            ->addOption(
                'max-queue-processes',
                null,
                InputOption::VALUE_OPTIONAL,
                'Quantidade máxima de processos gerados por fila'
            )
            ->addOption(
                'logger',
                null,
                InputOption::VALUE_OPTIONAL,
                'Sistema de logger, default php://stdout'
            )
            ->addOption(
                'logger-config',
                null,
                InputOption::VALUE_OPTIONAL,
                'Configuração do sistema de logs'
            )
            ->addOption(
                'max-processes',
                null,
                InputOption::VALUE_OPTIONAL,
                'Quantidade máxima de processos gerados especifica por fila'
            )
            ->setDescription('Run queuer supervisor');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $queue = $input->getOption('queue');
        $queueConfig = json_decode($input->getOption('queue-config'), true);
        $prefixTopic = $input->getOption('prefix-topic');
        $prefixExcludeTopic = $input->getOption('prefix-exclude-topic');
        $appConsole = $input->getOption('app-console');
        $maxQueueProcesses = $input->getOption('max-queue-processes');
        $topicListRefreshInSeconds = $input->getOption('topic-list-refresh');
        $logger = $input->getOption('logger');
        $loggerConfig = json_decode($input->getOption('logger-config'), true);
        $maxTopicProcesses = json_decode($input->getOption('max-processes'), true);

        $configurationFile = $input->getOption('config');
        $config = $this->yamlParser($configurationFile);

        if ($config) {
            $queue = $config['queue']['type'];
            $queueConfig = $config['queue']['config'];
            $prefixTopic = $config['queue']['prefix-topic'];
            $prefixExcludeTopic = $config['queue']['prefix-exclude'];
            $appConsole = $config['queuer']['app-console'];
            $maxQueueProcesses = $config['queuer']['max-queue-processes'];
            $topicListRefreshInSeconds = $config['queuer']['topic-list-refresh'];
            $logger = $config['logger']['type'];
            $loggerConfig = $config['logger']['config'];
            $maxTopicProcesses = $config['queuer']['max-processes'] ?? null;
        }

        if (!$config && (!$queue || !$appConsole || !$queueConfig)) {
            throw new \RuntimeException('Invalid configuration, RTFM!');
        }

        $logger = LoggerFactory::create($logger, $loggerConfig);

        $output->writeln('<info>Queuer: running</info>');

        $supervisorConfig = new Config([
            'queuer' => [
                'app-console' => $appConsole,
                'max-queue-processes' => $maxQueueProcesses,
                'topic-list-refresh' => $topicListRefreshInSeconds,
                'max-processes' => $maxTopicProcesses,
            ],
            'queue' => [
                'type' => $queue,
                'prefix-topic' => $prefixTopic,
                'prefix-exclude' => $prefixExcludeTopic,
                'config' => $queueConfig
            ],
            'logger' => [
                'type' => $logger,
                'config' => $loggerConfig
            ]
        ]);

        $supervisor = new Supervisor(
            $logger,
            $supervisorConfig,
            $configurationFile
        );

        $supervisor->run();
    }

    private function yamlParser($ymlPath)
    {
        if (!$ymlPath) {
            return null;
        }

        $yamlContent = file_get_contents($ymlPath);
        $yamlParsed = Yaml::parse($yamlContent);

        return $yamlParsed;
    }
}
