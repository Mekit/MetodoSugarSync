<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 17.35
 */

namespace Mekit\Command;

use Mekit\Console\Configuration;
use Mekit\Sync\SyncInterface;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class SyncDownCommand extends Command implements CommandInterface {
  const COMMAND_NAME = 'sync:down';
  const COMMAND_DESCRIPTION = 'Synchronize downwards';

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
          'config_file', InputArgument::REQUIRED, 'The yaml(.yml) configuration file inside the "' . $this->configDir
                                                  . '" subfolder.'
        ),
      ]
    );
  }

  /**
   * @param InputInterface $input
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
    $cfg = $cfg["commands"][static::COMMAND_NAME];
    if(!isset($cfg['sync'])) {
      throw new \LogicException("No 'sync' key is defined for the command '".static::COMMAND_NAME."'!");
    }
  }

  /**
   * Execute Command - call the syncDown() method on all classes listed in "sync" configuration
   */
  protected function executeCommand() {
    $cfg = Configuration::getConfiguration();
    $commandCfg = $cfg["commands"][static::COMMAND_NAME];
    $syncToolClasses = $commandCfg['sync'];
    foreach($syncToolClasses as $syncToolClass) {
      if(!class_exists($syncToolClass)) {
        throw new \LogicException("There is no class '".$syncToolClass."' by this name!");
      }
      if (!in_array('Mekit\Sync\SyncInterface', class_implements($syncToolClass))) {
        throw new \LogicException("Class '".$syncToolClass."' must implement 'Mekit\\MetodoSync\\SyncInterface'!");
      }

      /** @var SyncInterface $syncTool */
      $syncTool = new $syncToolClass([$this, 'log']);
      $syncTool->syncDown();
    }
  }
}