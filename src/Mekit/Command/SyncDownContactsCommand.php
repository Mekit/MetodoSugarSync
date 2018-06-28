<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 17.35
 */

namespace Mekit\Command;

use Mekit\Sync\MetodoToCrm\ContactData;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class SyncDownContactsCommand extends Command implements CommandInterface {
    const COMMAND_NAME = 'sync-down:contacts';
    const COMMAND_DESCRIPTION = 'Synchronize Contacts Metodo -> CRM';

    public function __construct() {
        parent::__construct(NULL);
    }

    /**
     * Configure command
     */
    protected function configure() {
        $this->setName(static::COMMAND_NAME);
        $this->setDescription(static::COMMAND_DESCRIPTION);
        $this->setDefinition(
            [
                new InputArgument(
                    'config_file', InputArgument::REQUIRED,
                    'The yaml(.yml) configuration file inside the "' . $this->configDir . '" subfolder.'
                ),
                new InputOption(
                    'delete-cache', '', InputOption::VALUE_NONE,
                    'Throw cache away?'
                ),
                new InputOption(
                    'invalidate-cache', '', InputOption::VALUE_NONE,
                    'Reset timestamps on cache so that updates will occur again?'
                ),
                new InputOption(
                    'invalidate-local-cache', '', InputOption::VALUE_NONE,
                    'Reset timestamps on local cache so that updates will occur again.'
                ),
                new InputOption(
                    'invalidate-remote-cache', '', InputOption::VALUE_NONE,
                    'Reset timestamps on remote cache so that updates will occur again.'
                ),
                new InputOption(
                    'update-cache', NULL, InputOption::VALUE_NONE,
                    'Update local cache?'
                ),
                new InputOption(
                    'update-remote', NULL, InputOption::VALUE_NONE,
                    'Update remote?'
                ),
            ]
        );
    }

    /**
     * @param \Symfony\Component\Console\Input\InputInterface   $input
     * @param \Symfony\Component\Console\Output\OutputInterface $output
     * @return bool
     * @throws \Exception
     */
    protected function execute(InputInterface $input, OutputInterface $output) {
        parent::_execute($input, $output);
        $this->log("Starting command " . static::COMMAND_NAME . "...");
        $dataClass = $this->getDataClass();
      $dataClass->execute($input->getOptions(), $input->getArguments());
        $this->log("Command " . static::COMMAND_NAME . " done.");
        return TRUE;
    }

    /**
     * @return ContactData
     */
    protected function getDataClass() {
        $class = "Mekit\\Sync\\MetodoToCrm\\ContactData";
        /** @var ContactData $dataClass */
        $dataClass = new $class([$this, 'log']);
        return $dataClass;
    }
}