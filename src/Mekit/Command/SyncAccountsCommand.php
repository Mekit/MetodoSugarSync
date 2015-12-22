<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 17.35
 */

namespace Mekit\Command;

use Mekit\Sync\Metodo\Down\AccountData;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class SyncAccountsCommand extends Command implements CommandInterface {
    const COMMAND_NAME = 'sync:accounts';
    const COMMAND_DESCRIPTION = 'Synchronize Accounts';

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
                    'config_file', InputArgument::REQUIRED, 'The yaml(.yml) configuration file inside the "'
                                                            . $this->configDir
                                                            . '" subfolder.'
                ),
            ]
        );
    }

    /**
     * @param InputInterface  $input
     * @param OutputInterface $output
     * @return bool
     */
    protected function execute(InputInterface $input, OutputInterface $output) {
        parent::_execute($input, $output);
        $this->log("Starting command " . static::COMMAND_NAME . "...");
        $this->log("INPUT: " . json_encode($input->getArguments()));
        $this->log("Command " . static::COMMAND_NAME . " done.");
    }

    /**
     * Execute Command
     */
    protected function executeCommand() {
        $accountData = new AccountData([$this, 'log']);
        $accountData->execute();
    }
}