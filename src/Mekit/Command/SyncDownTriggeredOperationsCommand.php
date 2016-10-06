<?php
/**
 * Created by Adam Jakab.
 * Date: 12/07/16
 * Time: 9.53
 */

namespace Mekit\Command;

use Mekit\Console\Configuration;
use Mekit\Sync\TriggeredOperations\OperationsEnumerator;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class SyncDownTriggeredOperationsCommand extends Command implements CommandInterface
{
  const COMMAND_NAME = 'sync-down:triggered-operations';
  const COMMAND_DESCRIPTION = 'Synchronize Triggered Operations Metodo -> CRM';

  public function __construct()
  {
    parent::__construct(NULL);
  }

  /**
   * Configure command
   */
  protected function configure()
  {
    $this->setName(static::COMMAND_NAME);
    $this->setDescription(static::COMMAND_DESCRIPTION);
    $this->setDefinition(
      [
        new InputArgument(
          'config_file', InputArgument::REQUIRED, 'The yaml(.yml) configuration file inside the "' . $this->configDir
                                                  . '" subfolder.'
        ),
        /*
        new InputOption(
          'dry', '', InputOption::VALUE_NONE, 'Show what would be done without actually executing anything'
        ),*/
      ]
    );
  }

  /**
   * @param \Symfony\Component\Console\Input\InputInterface   $input
   * @param \Symfony\Component\Console\Output\OutputInterface $output
   * @return bool
   * @throws \Exception
   */
  protected function execute(InputInterface $input, OutputInterface $output)
  {
    parent::_execute($input, $output);
    $this->log("Starting command " . static::COMMAND_NAME . "...");
    //
    $operationsEnumerator = new OperationsEnumerator([$this, 'log']);
    $operationsEnumerator->execute($input->getOptions());
    //
    $this->log("Command " . static::COMMAND_NAME . " done.");
    return TRUE;
  }
}
