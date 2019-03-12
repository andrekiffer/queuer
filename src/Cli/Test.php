<?php

namespace Queuer\Cli;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;


class Test extends Command
{
    protected function configure()
    {
        $this
            ->setName('test')
            ->addOption(
                'sleep',
                null,
                InputOption::VALUE_REQUIRED
            )
            ->addOption(
                'payload',
                null,
                InputOption::VALUE_REQUIRED
            )
            ->setDescription('Run queuer supervisor');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $sleep = $input->getOption('sleep');
        $payload = $input->getOption('payload');
        $output->writeln($payload);
        sleep((int) $sleep);
    }
}
