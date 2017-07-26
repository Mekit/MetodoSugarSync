<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 17.35
 */

namespace Mekit\Command;

use Mekit\Sync\CsvToCrm\GenericAccounts;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class ImportCsvUpdateAccountsCommand extends Command implements CommandInterface
{
  const COMMAND_NAME = 'csv:update-account';
  const COMMAND_DESCRIPTION = 'Update Accounts from csv';

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
        new InputOption(
          'update-remote', NULL, InputOption::VALUE_NONE, 'Update remote?'
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
  protected function execute(InputInterface $input, OutputInterface $output)
  {
    parent::_execute($input, $output);
    $this->log("Starting command " . static::COMMAND_NAME . "...");
    $dataClass = $this->getDataClass();
    $dataClass->execute($input->getOptions(), $input->getArguments());
    $this->log("Command " . static::COMMAND_NAME . " done.");
    return TRUE;
  }

  /**
   * @return GenericAccounts
   */
  protected function getDataClass()
  {
    $class = "Mekit\\Sync\\CsvToCrm\\GenericAccounts";
    /** @var GenericAccounts $dataClass */
    $dataClass = new $class([$this, 'log']);
    return $dataClass;
  }
}