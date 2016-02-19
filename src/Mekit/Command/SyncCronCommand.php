<?php
/**
 * Created by Adam Jakab.
 * Date: 04/01/16
 * Time: 14.44
 */

namespace Mekit\Command;

use Mekit\Console\Configuration;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Cron\CronExpression;
use Symfony\Component\Process\Process;
use Symfony\Component\Process\Exception\ProcessFailedException;

class SyncCronCommand extends Command implements CommandInterface {
    const COMMAND_NAME = 'sync:cron';
    const COMMAND_DESCRIPTION = 'Cron command for continuous processing.';
    const COMMAND_HEARTBEAT = 5;

    /** @var  \DateTime */
    private $lastCommandExecutionCheckTime;

    /** @var array */
    private $executionList = [];

    public function __construct() {
        parent::__construct(NULL);
        $this->lastCommandExecutionCheckTime = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
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
        while (TRUE) {
            $this->checkForEndedProcesses();
            $this->checkCommandsToExecute();
            sleep(self::COMMAND_HEARTBEAT);
        }
        return TRUE;
    }

    protected function checkForEndedProcesses() {
        /**
         * @var string  $processName
         * @var Process $process
         */
        foreach ($this->executionList as $processName => $process) {
            if (!$process->isRunning()) {
                unset($this->executionList[$processName]);
                $this->log("Process Ended[" . $processName . "].");
                if (!$process->isSuccessful()) {
                    $this->log("Process Error Output: " . $process->getErrorOutput());
                }
            }
        }
    }

    protected function checkCommandsToExecute() {
        $now = new \DateTime();
        $interval = $this->lastCommandExecutionCheckTime->diff($now, TRUE);
        $secondsSinceLastExecution = (60 * (int) $interval->format("%i")) + (int) $interval->format("%s");
        if ($secondsSinceLastExecution >= 60) {
            $this->lastCommandExecutionCheckTime = $now;
            //
            $cfg = Configuration::getConfiguration();

            //$this->log("CMD LIST: " . json_encode($cfg["commands"]));

            if (isset($cfg["commands"]) && count($cfg["commands"])) {
                foreach ($cfg["commands"] as $command) {
                    if (!array_key_exists($command["name"], $this->executionList)) {
                        $cron = CronExpression::factory($command["execution_times"]);
                        if ($cron->isDue()) {
                            $this->executeCronCommand($command);
                        }
                        else {
                            $this->log(
                                "Next Run[" . $command["name"] . "]: " . $cron->getNextRunDate()
                                    ->format('Y-m-d H:i:s')
                            );
                        }
                    }
                }
            }
        }
    }

    protected function executeCronCommand($command) {
        $processName = $command["name"];
        $commandToExecute = $command["command"] . " " . implode(" ", $command["options"]);
        $process = new Process($commandToExecute, NULL, NULL, NULL, NULL);
        $this->executionList[$processName] = $process;
        $process->start();
        $this->log("Process Started[" . $processName . "](" . $process->getPid() . ")...");
    }

}