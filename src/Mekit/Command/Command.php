<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 14.27
 */

namespace Mekit\Command;

use Mekit\Console\Configuration;
use Symfony\Component\Console\Command\Command as ConsoleCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class Command extends ConsoleCommand
{
  /** @var string  */
  protected $configDir = 'config';

  /** @var  InputInterface */
  protected $cmdInput;

  /** @var  OutputInterface */
  protected $cmdOutput;

  /**
   * @param string $name
   */
  public function __construct($name = null) {
    parent::__construct($name);
  }

  protected function _execute(InputInterface $input, OutputInterface $output) {
    $this->cmdInput = $input;
    $this->cmdOutput = $output;
    $this->parseConfiguration();
  }


  /**
   * @return string
   */
  protected function getTemporaryFileName() {
    $cfg = Configuration::getConfiguration();
    return $cfg["temporary_path"] . "/" . md5("temporary-file-".microtime()).'.txt';
  }

  /**
   * Parse yml configuration
   */
  protected function parseConfiguration() {
    $config_file = __DIR__ . '/../../../' . $this->configDir . '/' . $this->cmdInput->getArgument('config_file');
    Configuration::initializeWithConfigurationFile($config_file);
  }

  /**
   * @param string $msg
   */
  public function log($msg) {
    $cfg = Configuration::getConfiguration();
    if(isset($cfg['global']['log_to_console']) && $cfg['global']['log_to_console']) {
      $this->cmdOutput->writeln($msg);
    }
  }

}