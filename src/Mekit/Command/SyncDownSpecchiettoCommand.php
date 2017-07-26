<?php
/**
 * Created by Adam Jakab.
 * Date: 25/07/17
 * Time: 10.06
 */

namespace Mekit\Command;


use Mekit\Sync\MetodoToCrm\AccountData;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class SyncDownSpecchiettoCommand extends Command implements CommandInterface
{
  const COMMAND_NAME = 'sync-down:specchietto';
  const COMMAND_DESCRIPTION = 'Synchronize Specchietto Account Metodo -> CRM';

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
        new InputArgument(
          'client-code', InputArgument::OPTIONAL, NULL
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
   * @return AccountData
   */
  protected function getDataClass()
  {
    $class = "Mekit\\Sync\\MetodoToCrm\\SpecchiettoData";
    /** @var AccountData $dataClass */
    $dataClass = new $class([$this, 'log']);
    return $dataClass;
  }
}