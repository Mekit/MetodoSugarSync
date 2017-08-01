<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 14.27
 */

namespace Mekit\Command;

use Mekit\Console\Configuration;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Symfony\Component\Console\Command\Command as ConsoleCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class Command extends ConsoleCommand
{
  /** @var string */
  protected $configDir = 'config';

  /** @var  InputInterface */
  protected $cmdInput;

  /** @var  OutputInterface */
  protected $cmdOutput;

  /** @var  Logger */
  protected $logger;

  /** @var  \Swift_Mailer */
  protected $mailer;

  /** @var bool */
  protected $logToConsole = FALSE;

  /** @var bool */
  protected $logToFile = FALSE;

  /**
   * @param string $name
   */
  public function __construct($name = NULL)
  {
    parent::__construct($name);
  }

  protected function _execute(InputInterface $input, OutputInterface $output)
  {
    $this->cmdInput = $input;
    $this->cmdOutput = $output;
    $this->setConfigurationFile();
    //$this->setupMailer();
    $this->setupLogger();
  }

  /**
   * Parse yml configuration
   */
  protected function setConfigurationFile()
  {
    $config_file = $this->cmdInput->getArgument('config_file');
    $configPath = realpath($config_file);
    if (!$configPath)
    {
      $configPath = realpath(PROJECT_ROOT . '/config/' . $config_file);
    }
    if (!$configPath)
    {
      throw new \InvalidArgumentException("The configuration file does not exist!");
    }
    Configuration::initializeWithConfigurationFile($configPath);
  }

  /**
   *
   */
  protected function setupLogger()
  {
    $cfg = Configuration::getConfiguration();
    $this->logToConsole = isset($cfg['global']['log_to_console']) && $cfg['global']['log_to_console'];
    $this->logToFile = isset($cfg['global']['log_to_file']) && $cfg['global']['log_to_file'];
    //
    if ($this->logToFile)
    {
      $logFilePrefix = (isset($cfg['global']['log_file_prefix'])
                        && $cfg['global']['log_file_prefix'] ? $cfg['global']['log_file_prefix'] : "");
      $this->logger = new Logger("file_logger");
      //LOG HANDLER: FILE
      $today = new \DateTime();
      $logFilePath = PROJECT_ROOT . DIRECTORY_SEPARATOR . 'log' . DIRECTORY_SEPARATOR . $logFilePrefix
                     . $today->format("Y-m-d") . '.txt';
      $logToFileHandler = new StreamHandler($logFilePath, Logger::INFO);
      $this->logger->pushHandler($logToFileHandler);
      //
      //IF YOU WANT MAILING - ENABLE THIS
      //LOG HANDLER: MAIL
      /** @var \Swift_Message $message */
      /*
      $message = \Swift_Message::newInstance($cfg['swiftmailer']['message']['subject'])
          ->setFrom($cfg['swiftmailer']['message']['from'])
          ->setTo($cfg['swiftmailer']['message']['to'])
          ->setBody('...this will be replaced...')
      ;
      $logToMailHandler = new SwiftMailerHandler($this->mailer, $message);
      $this->logger->pushHandler($logToMailHandler);*/
    }
  }

  protected function setupMailer()
  {
    $cfg = Configuration::getConfiguration();
    // Create the Transport
    $transport = \Swift_SmtpTransport::newInstance($cfg['swiftmailer']['server'], $cfg['swiftmailer']['port'])
      ->setUsername($cfg['swiftmailer']['username'])
      ->setPassword($cfg['swiftmailer']['password']);
    // Create the Mailer
    $this->mailer = \Swift_Mailer::newInstance($transport);
  }

  /**
   * @param string $msg
   * @param int    $level
   * @param array  $context
   */
  public function log($msg, $level = Logger::INFO, $context = [])
  {
    if ($this->logToConsole)
    {
      $this->cmdOutput->writeln($msg);
    }
    if ($this->logToFile)
    {
      $this->logger->addRecord($level, $msg, $context);
    }

  }
}