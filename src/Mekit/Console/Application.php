<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 14.21
 */

namespace Mekit\Console;

use Symfony\Component\Console\Application as BaseApplication;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;


class Application extends BaseApplication {

    /**
     * @param string $name
     * @param string $version
     */
    public function __construct($name, $version) {
        parent::__construct($name, $version);
        $commands = $this->enumerateCommands();
        foreach ($commands as $command) {
            $this->add(new $command);
        }
    }

    /**
     * Runs the current application.
     *
     * @param InputInterface  $input An Input instance
     * @param OutputInterface $output An Output instance
     *
     * @return int 0 if everything went fine, or an error code
     *
     * @throws \Exception When doRun returns Exception
     *
     * @api
     */
    public function run(InputInterface $input = NULL, OutputInterface $output = NULL) {
        //@todo: 1) we need to check lock file - if exists refuse because another instance is running
        //@todo: 2) we need to write lock file
        $res = parent::run($input, $output);
        //@todo: 2) we need to delete lock file
        return $res;
    }

    /**
     * @return array
     */
    protected function enumerateCommands() {
        $answer = [];
        $commandsPath = realpath(__DIR__ . '/../Command');
        $commandFiles = glob($commandsPath . '/*Command.php');
        foreach ($commandFiles as &$commandFile) {
            $commandClass = 'Mekit\\Command\\' . str_replace(
                    '.php', '', str_replace(
                              $commandsPath . '/', '', $commandFile
                          )
                );
            if (in_array('Mekit\Command\CommandInterface', class_implements($commandClass))) {
                $answer[] = $commandClass;
            }
        }
        return $answer;
    }
}

