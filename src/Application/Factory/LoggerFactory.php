<?php

namespace Queuer\Application\Factory;

use Elastica\Client;
use Monolog\Handler\ElasticSearchHandler;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Monolog\Logger as MonologLogger;
use Monolog\Processor\MemoryPeakUsageProcessor;
use Monolog\Processor\MemoryUsageProcessor;
use Bugsnag;

class LoggerFactory
{
    public static function create(string $loggerService = null, array $loggerConfig = null)
    {
        $channel = $loggerConfig['channel'] ?? 'queuer';
        $logger  = new Logger($channel);
        $handler = new StreamHandler('php://stdout', Logger::DEBUG);

        $logger->pushHandler($handler);

        switch ($loggerService) {
            case 'elk':
                $loggerConfig['aws_access_key_id'] = $loggerConfig['aws_access_key_id'] ?? null;
                if ($loggerConfig['transport'] === 'AwsAuthV4' && empty($loggerConfig['aws_access_key_id'])) {
                    $loggerConfig['transport'] = 'Https';
                    unset(
                        $loggerConfig['aws_access_key_id'],
                        $loggerConfig['aws_secret_access_key'],
                        $loggerConfig['aws_region']
                    );
                } elseif ($loggerConfig['transport'] === 'AwsAuthV4') {
                    $loggerConfig['headers'] = [
                        'Content-Type' => 'application/json'
                    ];
                }
                $client = new Client($loggerConfig);
                $options = [
                    'index' => 'logstash-queuer',
                    'type' => 'logs',
                    'ignore_error' => true
                ];

                $streamHandler = new ElasticSearchHandler($client, $options, MonologLogger::DEBUG);
                $logger->pushHandler($streamHandler);
                $logger->pushProcessor(new MemoryPeakUsageProcessor);
                $logger->pushProcessor(new MemoryUsageProcessor);
                break;
            case 'bugsnag':
                $bugsnag = Bugsnag\Client::make($loggerConfig['key']);
                Bugsnag\Handler::register($bugsnag);
                $bugsnag->getConfig()->setBatchSending(false);

                $bugsnagLogger = new Bugsnag\PsrLogger\BugsnagLogger($bugsnag);
                $logger = new Bugsnag\PsrLogger\MultiLogger([$bugsnagLogger, $logger]);

                $releaseStage = $loggerConfig['release-stage'] ?? 'dev';
                $notifyReleaseStages = $loggerConfig['notify-release-stages'] ?? ['prod'];

                $bugsnag->getConfig()->setReleaseStage($releaseStage);
                $bugsnag->getConfig()->setNotifyReleaseStages($notifyReleaseStages);
                break;
        }

        return $logger;
    }
}
