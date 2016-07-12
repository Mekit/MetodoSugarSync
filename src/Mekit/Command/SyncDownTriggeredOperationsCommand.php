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

  /** @var array */
  protected $databases = ['IMP', 'MEKIT', 'Crm2Metodo'];

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
          'dry', '', InputOption::VALUE_NONE, 'Show what would be done without actually executing anything'
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
    $tableMap = $this->setupTableMap();
    //print_r($tableMap);
    //
    $operationsEnumerator = new OperationsEnumerator([$this, 'log']);
    $operationsEnumerator->setTableMap($tableMap);
    $operationsEnumerator->execute($input->getOptions());
    //
    $this->log("Command " . static::COMMAND_NAME . " done.");
    return TRUE;
  }

  /**
   * @return array
   * @throws \Exception
   */
  protected function setupTableMap()
  {
    $cfg = Configuration::getConfiguration();
    if (!isset($cfg["table-map"]))
    {
      throw new \Exception("Missing 'table-map' key from configuration file!");
    }
    if (!is_array($cfg["table-map"]))
    {
      throw new \Exception("The 'table-map' key in configuration must be an array!");
    }
    $tableMap = [];
    foreach ($cfg["table-map"] as $tableName => $operationClassName)
    {
      $tableNameParts = $this->getMSSQLTableNameParts($tableName);
      $this->checkMSSQLTable($tableNameParts);
      $this->checkOperationClass($operationClassName);
      $tableMap[$tableName] = [
        'table-name-parts' => $tableNameParts,
        'operation-class-name' => $operationClassName,
      ];
    }
    return $tableMap;
  }

  /**
   * @param string $operationClassName
   * @throws \Exception
   */
  protected function checkOperationClass($operationClassName)
  {
    if (!class_exists($operationClassName))
    {
      throw new \Exception("Inexistent operation class(" . $operationClassName . ")!");
    }
    $reflection = new \ReflectionClass($operationClassName);
    if (!$reflection->implementsInterface('Mekit\Sync\TriggeredOperations\TriggeredOperationInterface'))
    {
      throw new \Exception(
        "Operation class(" . $operationClassName . ") does not implement TriggeredOperationInterface!"
      );
    }
    if ($reflection->getParentClass()->getName() != 'Mekit\Sync\TriggeredOperations\TriggeredOperation')
    {
      throw new \Exception("Operation class(" . $operationClassName . ") does not extend TriggeredOperation!");
    }
  }

  /**
   * Makes sure this database table exists
   * @param array $tableNameParts
   * @throws \Exception
   */
  protected function checkMSSQLTable($tableNameParts)
  {
    $db = Configuration::getDatabaseConnection("SERVER2K8");

    $informationSchemaTableName = $tableNameParts['catalog'] . '.INFORMATION_SCHEMA.TABLES';
    
    $sql = "SELECT * FROM " . $informationSchemaTableName . "
            WHERE
            TABLE_CATALOG LIKE :catalog
            AND
            TABLE_SCHEMA LIKE :schema
            AND
            TABLE_NAME LIKE :tablename
            ";
    $st = $db->prepare($sql);
    $st->execute(
      [
        ':catalog' => $tableNameParts['catalog'],
        ':schema' => $tableNameParts['schema'],
        ':tablename' => $tableNameParts['table-name'],
      ]
    );
    $items = $st->fetchAll(\PDO::FETCH_OBJ);
    if (count($items) == 0)
    {
      throw new \Exception("Inexistent database table(" . $tableNameParts['full-table-name'] . ")!");
    }
    if (count($items) > 1)
    {
      throw new \Exception("Multiple database tables(" . $tableNameParts['full-table-name'] . ")!");
    }
  }

  /**
   * @param $tableName - like: IMP.dbo.TESTEDOCUMENTI
   * @return array
   * @throws \Exception
   */
  protected function getMSSQLTableNameParts($tableName)
  {
    $answer = [];
    $answer['full-table-name'] = $tableName;
    $parts = explode(".", $tableName);
    if (count($parts) != 3)
    {
      throw new \Exception("Invalid table name($tableName) in 'table-map'!");
    }
    if (!in_array($parts[0], $this->databases))
    {
      throw new \Exception("Invalid database name($parts[0]) in table name($tableName) in 'table-map'!");
    }
    $answer['catalog'] = $parts[0];
    $answer['schema'] = $parts[1];
    $answer['table-name'] = $parts[2];
    return $answer;
  }

}