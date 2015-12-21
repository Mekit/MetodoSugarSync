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

class Command extends ConsoleCommand {
    /** @var string */
    protected $configDir = 'config';

    /** @var  InputInterface */
    protected $cmdInput;

    /** @var  OutputInterface */
    protected $cmdOutput;

    /** @var  Logger */
    protected $logger;

    /**
     * @param string $name
     */
    public function __construct($name = NULL) {
        parent::__construct($name);
    }

    protected function _execute(InputInterface $input, OutputInterface $output) {
        $this->cmdInput = $input;
        $this->cmdOutput = $output;
        $this->setupLogger();
        $this->setConfigurationFile();
    }


    /**
     * @return string
     */
    protected function getTemporaryFileName() {
        $cfg = Configuration::getConfiguration();
        return $cfg["temporary_path"] . "/" . md5("temporary-file-" . microtime()) . '.txt';
    }

    /**
     * Parse yml configuration
     */
    protected function setConfigurationFile() {
        $config_file = $this->cmdInput->getArgument('config_file');
        $configPath = realpath($config_file);
        if (!$configPath) {
            $configPath = realpath(PROJECT_ROOT . '/config/' . $config_file);
        }
        if (!$configPath) {
            throw new \InvalidArgumentException("The configuration file does not exist!");
        }
        Configuration::initializeWithConfigurationFile($configPath);
    }

    protected function setupLogger() {
        $this->logger = new Logger("file_logger");
        $today = new \DateTime();
        $logFilePath = PROJECT_ROOT . '/log/' . $today->format("Y-m-d H:i:s") . '.txt';
        $logHandler = new StreamHandler($logFilePath, Logger::INFO);
        $this->logger->pushHandler($logHandler);
    }

    /**
     * @param string $msg
     */
    public function log($msg) {
        $cfg = Configuration::getConfiguration();
        if (isset($cfg['global']['log_to_console']) && $cfg['global']['log_to_console']) {
            $this->cmdOutput->writeln($msg);
        }
        $this->logger->addInfo($msg);
    }

}