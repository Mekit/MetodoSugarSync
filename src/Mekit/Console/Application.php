<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 14.21
 */

namespace Mekit\Console;

use Symfony\Component\Console\Application as BaseApplication;


class Application extends BaseApplication
{

  /**
   * @param string $name
   * @param string $version
   */
  public function __construct($name, $version)
  {
    parent::__construct($name, $version);
    $commands = $this->enumerateCommands();
    foreach($commands as $command) {
      $this->add(new $command);
    }
  }

  /**
   * @return array
   */
  protected function enumerateCommands() {
    $answer = [];
    $commandsPath = realpath(__DIR__ . '/../Command');
    $commandFiles = glob($commandsPath.'/*Command.php');
    foreach($commandFiles as &$commandFile) {
      $commandClass = 'Mekit\\Command\\' . str_replace('.php' , '', str_replace($commandsPath . '/', '', $commandFile));
      if (in_array('Mekit\Command\CommandInterface', class_implements($commandClass))) {
        $answer[] = $commandClass;
      }
    }
    return $answer;
  }
}

