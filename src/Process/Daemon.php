<?php

declare(strict_types=1);
declare(ticks=1);

namespace Queuer\Process;

final class Daemon
{
    public static function run(
        callable $callback,
        $allowStop = false
    ) {
        /**
         * Flag para indicar se é possível finalizar a execução de forma segura
         *
         * @var bool
         */
        $isRequestedStop = false;

        if (! function_exists('pcntl_signal')) {
            throw new \RuntimeException('Extension "pcntl_signal" not instaled');
        }

        $handler = function ($sigNumber) use (&$isRequestedStop) {
            if (in_array($sigNumber, [SIGTERM, SIGHUP, SIGINT])) {
                echo 'Parando worker de forma segura, aguarde...' . PHP_EOL;
                $isRequestedStop = true;
            }
        };

        if ($allowStop === false) {
            pcntl_signal(SIGTERM, $handler);
            pcntl_signal(SIGHUP, $handler);
            pcntl_signal(SIGINT, $handler);
        }

        while (true) {
            try {
                $callback();
            } catch (\Throwable $e) {
                echo $e->getMessage() . PHP_EOL;
            }

            sleep(1);

            if ($isRequestedStop) {
                exit();
            }
        }
    }
}
