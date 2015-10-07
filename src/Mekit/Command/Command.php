<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 14.27
 */

namespace Mekit\Command;

use Symfony\Component\Console\Command\Command as ConsoleCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Yaml\Parser;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Filesystem\Exception\IOException;

class Command extends ConsoleCommand
{
  /** @var string  */
  protected $configDir = 'config';

  /** @var  Array */
  protected $configuration;

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
    return $this->configuration["temporary_path"] . "/" . md5("temporary-file-".microtime()).'.txt';
  }

  /**
   * Parse yml configuration
   */
  protected function parseConfiguration() {
    $config_file = $this->configDir . '/' . $this->cmdInput->getArgument('config_file');
    if(!file_exists($config_file)) {
      throw new \InvalidArgumentException("The configuration file does not exist!");
    }
    $yamlParser = new Parser();
    $config = $yamlParser->parse(file_get_contents($config_file));
    if(!is_array($config) || !isset($config["config"])) {
      throw new \InvalidArgumentException("Malformed configuration file!");
    }
    $this->configuration = $config["config"];

    //Temporary path checks & creation
    if(!isset($this->configuration["global"]["temporary_path"])) {
      throw new \LogicException("Missing 'temporary_path' configuration in 'global' section!");
    } else {
      $fs = new Filesystem();
      if(!$fs->exists($this->configuration["global"]["temporary_path"])) {
        try {
          $fs->mkdir($this->configuration["global"]["temporary_path"]);
        } catch(IOException $e) {
          throw new \LogicException("Unable to create 'temporary_path'(".$this->configuration["global"]["temporary_path"].")!");
        }
      }
    }
  }


  protected function getGlobalConfiguration() {
    return $this->configuration['global'];
  }

  protected function getCommandConfiguration($command) {
    return isset($this->configuration['commands'][$command]) ? $this->configuration['commands'][$command] : false;
  }

  /**
   * @param string $msg
   */
  protected function log($msg) {
    $log2console = isset($this->configuration['global']['log_to_console']) && $this->configuration['global']['log_to_console'];
    if($log2console) {
      $this->cmdOutput->writeln($msg);
    }
  }

}