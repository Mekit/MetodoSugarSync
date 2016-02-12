<?php
/**
 * Created by Adam Jakab.
 * Date: 11/02/16
 * Time: 11.51
 */

namespace Mekit\Command;

use Mekit\Sync\CrmToMetodo\ContactData;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;


class SyncUpContactsCommand extends Command implements CommandInterface {
    const COMMAND_NAME = 'sync-up:contacts';
    const COMMAND_DESCRIPTION = 'Synchronize Contacts CRM -> Metodo';

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
                )
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
        $dataClass->execute($input->getOptions());
        $this->log("Command " . static::COMMAND_NAME . " done.");
        return TRUE;
    }

    /**
     * @return ContactData
     */
    protected function getDataClass() {
        $class = "Mekit\\Sync\\CrmToMetodo\\ContactData";
        /** @var ContactData $dataClass */
        $dataClass = new $class([$this, 'log']);
        return $dataClass;
    }
}