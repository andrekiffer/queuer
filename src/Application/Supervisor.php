<?php

namespace Queuer\Application;

use Psr\Log\LoggerInterface;
use Queuer\Application\Command\ExecuteTask;
use Queuer\Application\Factory\QueuerManagerFactory;
use Queuer\Exception\InvalidQueuerMessageException;
use React\ChildProcess\Process;
use React\EventLoop\Factory;
use Symfony\Component\Yaml\Yaml;
use Zend\Config\Config;

class Supervisor
{
    const DEFAULT_TOPIC_LIST_REFRESH = 60; //seconds

    /** @var QueueManager */
    private $queueManager;
    /** @var Config */
    private $config;
    /** @var LoggerInterface */
    private $logger;
    /** @var array */
    private $processes;
    /** @var mixed */
    private $loop;
    /** @var string */
    private $configurationYamlFile;
    /** @var null|int */
    private $timestampConfigurationFile;
    /** @var array  */
    private $maxProcessesPrefix = [];

    public function __construct(
        LoggerInterface $logger,
        Config $config,
        string $configurationYamlFile = null
    ) {
        $this->loop = Factory::create();
        $this->queueManager = QueuerManagerFactory::create($config->get('queue')->toArray());
        $this->logger = $logger;
        $this->config = $config;
        $this->processes = [];
        $this->configurationYamlFile = $configurationYamlFile;
        $this->timestampConfigurationFile = $configurationYamlFile ? fileatime($configurationYamlFile) : null;
    }

    public function run()
    {
        $this->setMaxProcessesPrefix();
        $this->refreshConfigurationYamlFile();
        $this->refreshTopics();
        $this->logMetrics();
        $this->listenQueues();
        $this->loop->run();
    }

    private function setMaxProcessesPrefix()
    {
        $maxTopics = $this->config->get('queuer')->get('max-processes');
        if ($maxTopics) {
            foreach ($maxTopics as $prefix => $max) {
                $this->maxProcessesPrefix[$prefix] = (int) $max;
            }
        }
    }

    private function getMaxProcessesByPrefix($topic)
    {
        $topic = strtolower($topic);
        foreach ($this->maxProcessesPrefix as $prefix => $max) {
            $prefix = strtolower($prefix);
            if (\substr($topic, 0, \strlen($prefix)) === $prefix) {
                return $max;
            }
        }
        return null;
    }

    private function listenQueues()
    {
        $manager = $this->queueManager;

        $this->loop->addPeriodicTimer(2, function () use ($manager) {
            $topics = $manager->topics();
            foreach ($topics as $topic) {
                if ($this->hasAnyProcessFromTopic($topic)) {
                    continue;
                }

                try {
                    $commandsFromTopic = $manager->getCommandsFromTopic($topic);
                } catch (\Throwable $e) {
                    $this->logger->error('Could not receive messages from topic: ' . $topic);
                    $this->queueManager->refreshTopics();
                    continue;
                }

                foreach ($commandsFromTopic as $command) {
                    if ($this->exceededTheMaximumNumberOfProcesses($topic)) {
                        continue;
                    }

                    try {
                        $commandWithMessageId = ['message_id' => $command['message_id']] + $command['data'];
                        $executeTaskCommand = ExecuteTask::from($commandWithMessageId);
                    } catch (InvalidQueuerMessageException|\Throwable $e) {
                        $this->logger->error('Invalid Queuer Message', [
                            'exception' => $e->getMessage(),
                            'message_data' => json_encode($commandWithMessageId)
                        ]);
                        continue;
                    }

                    $this->spawnProcess($topic, $executeTaskCommand);
                }
            }
        });
    }

    private function logMetrics()
    {
        $this->loop->addPeriodicTimer(5, function () {
            $detailedCount = [];
            $total = 0;

            foreach ($this->processes as $topic => $process) {
                $total += count($process);
                $explodeSlash = (strpos($topic, '/')) !== false ? explode('/', $topic) : [$topic];
                $topic = end($explodeSlash);
                array_push($detailedCount, ['topic' => $topic, 'total' => count($process)]);
            }

            if (!$total) {
                return;
            }

            $this->logger->info('Metrics', [
                'total_processes' => $total,
                'total_processes_per_queue' => $detailedCount
            ]);
        });
    }

    private function registerProcess(string $topic, Process $process)
    {
        $topicHash = $topic;

        if (!isset($this->processes[$topicHash])) {
            $this->processes[$topicHash] = [];
        }

        $this->processes[$topicHash][$process->getPid()] = $process;
    }

    private function unregisterProcess(string $topic, Process $process)
    {
        $topicHash = $topic;

        if (!isset($this->processes[$topicHash])) {
            return;
        }

        if (isset($this->processes[$topicHash][$process->getPid()])) {
            unset($this->processes[$topicHash][$process->getPid()]);
        }

        if (!count($this->processes[$topicHash])) {
            unset($this->processes[$topicHash]);
        }
    }

    private function hasAnyProcessFromTopic(string $topic): bool
    {
        $topicHash = $topic;
        if (!isset($this->processes[$topicHash])) {
            return false;
        }

        return $this->exceededTheMaximumNumberOfProcesses($topic);
    }

    private function exceededTheMaximumNumberOfProcesses(string $topic)
    {
        if (!isset($this->processes[$topic])) {
            return false;
        }
        $shortTopic = $this->queueManager->getConsumer()->getShortTopic($topic);
        $total = \count($this->processes[$topic]);
        $maxQueueProcesses = $this->getMaxProcessesByPrefix($shortTopic);
        if (!$maxQueueProcesses) {
            $maxQueueProcesses = (int)$this->config->get('queuer')->get('max-queue-processes');
        }
        return ($total >= $maxQueueProcesses);
    }

    private function spawnProcess($topic, ExecuteTask $command)
    {
        $this->logger->info('Starting process', ['command_id' => $command->commandId()]);

        $process = new Process($command->getExecutionCommand($this->config->get('queuer')->get('app-console')));
        $process->start($this->loop);

        $this->registerProcess($topic, $process);

        $process->stdout->on('data', function ($chunk) use ($topic, $command) {
            $this->logger->info($chunk, [
                'topic' => $topic,
                'command_id' => $command->commandId()
            ]);
        });

        $process->stderr->on('data', function ($chunk) use ($topic, $command) {
            $this->logger->error($chunk, [
                'topic' => $topic,
                'command_id' => $command->commandId()
            ]);
        });

        $process->on('exit', function ($exitCode, $termSignal) use ($topic, $process, $command) {
            $this->unregisterProcess($topic, $process);

            if ($this->executionCompletedSuccessfully($exitCode)) {
                $this->removeMessageFromCommand($topic, $command);
            }

            $this->logger->debug('Process exited with code', [
                'topic' => $topic,
                'exit_code' => $exitCode,
                'term_signal' => $termSignal,
                'command_id' => $command->commandId()
            ]);
        });
    }

    private function removeMessageFromCommand(string $topic, ExecuteTask $command)
    {
        try {
            $this->queueManager->removeMessageFromQueue($topic, $command->messageId());
        } catch (\Throwable $e) {
            $this->logger->error('Could not delete message', [
                'topic' => $topic,
                'message_id' => $command->messageId()
            ]);
        }
    }

    private function executionCompletedSuccessfully($exitCode): bool
    {
        return $exitCode === 0;
    }

    private function refreshTopics()
    {
        $queueManager = $this->queueManager;
        $queueManager->refreshTopics();
        $this->logger->info('Topics updated successfully');

        $configSecondsToRefresh = $this->config->get('queuer')->get(
            'topic-list-refresh',
            self::DEFAULT_TOPIC_LIST_REFRESH
        );

        $this->loop->addPeriodicTimer($configSecondsToRefresh, function () use ($queueManager) {
            $totalOfTopics = $queueManager->count();
            $queueManager->refreshTopics();

            if ($totalOfTopics != $queueManager->count()) {
                $this->logger->info('Topics updated successfully');
            }
        });
    }

    private function refreshConfigurationYamlFile()
    {
        if (!$this->configurationYamlFile) {
            return;
        }

        $this->loop->addPeriodicTimer(5, function () {
            $yamlContent = file_get_contents($this->configurationYamlFile);
            $yamlParsed  = Yaml::parse($yamlContent);

            if ($yamlParsed == $this->config->toArray()) {
                return;
            }

            $newConfig   = new Config($yamlParsed);

            $this->config = $newConfig;
            $this->setMaxProcessesPrefix();
            $this->queueManager->updateConfiguration($newConfig->get('queue'));
            $this->logger->info('Configuration updated successfully');
        });
    }
}
