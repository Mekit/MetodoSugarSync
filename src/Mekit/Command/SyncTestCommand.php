<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 14.26
 */

namespace Mekit\Command;

use Mekit\Console\Configuration;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class SyncTestCommand extends Command implements CommandInterface
{
  const COMMAND_NAME = 'sync:test';
  const COMMAND_DESCRIPTION = 'Run some tests...';

  public function __construct() {
    parent::__construct(null);
  }

  /**
   * Configure command
   */
  protected function configure() {
    $this->setName(static::COMMAND_NAME);
    $this->setDescription(static::COMMAND_DESCRIPTION);
    $this->setDefinition(
      [
        new InputArgument('config_file', InputArgument::REQUIRED, 'The yaml(.yml) configuration file inside the "' . $this->configDir . '" subfolder.'),
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
    $this->checkConfiguration();
    $this->executeCommand();
    $this->log("Command " . static::COMMAND_NAME . " done.");
  }

  /**
   * Execute some configuration checks
   */
  protected function checkConfiguration() {
    $cfg = Configuration::getConfiguration();
    if(!isset($cfg["commands"][static::COMMAND_NAME])) {
      throw new \LogicException("No configuration is defined for the command '". static::COMMAND_NAME ."'!");
    }
  }

  /**
   * Execute Command
   */
  protected function executeCommand() {
    $this->log(static::COMMAND_NAME . " working...");
  }

}