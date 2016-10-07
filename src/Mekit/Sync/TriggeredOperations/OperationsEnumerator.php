<?php
/**
 * Created by Adam Jakab.
 * Date: 12/07/16
 * Time: 11.09
 */

namespace Mekit\Sync\TriggeredOperations;

use Mekit\Console\Configuration;
use Mekit\Exceptions\OperatorNotFoundException;
use Monolog\Logger;

/**
 * Class OperationsEnumerator
 * @package Mekit\Sync\TriggeredOperations
 */
class OperationsEnumerator
{
  /** @var callable */
  protected $logger;

  /** @var array */
  protected $tableMap = [];

  /** @var array */
  protected $databases = ['IMP', 'MEKIT', 'Crm2Metodo'];

  /** @var  \PDO */
  protected $db;

  /** @var  \PDOStatement */
  protected $localItemStatement;

  /** @var int */
  protected $maxOperationFails = 100000;

  /** @var int */
  protected $counter = 0;

  /**
   * OperationsEnumerator constructor.
   * @param callable $logger
   */
  public function __construct($logger)
  {
    $this->logger = $logger;
    $this->db = Configuration::getDatabaseConnection("SERVER2K8");
    $this->setupTableMap();
  }

  /**
   * @param array $options
   */
  public function execute($options)
  {
    //$this->log("Executing with options: " . json_encode($options));
    $FORCE_LIMIT = 999;
    while ($operationElement = $this->getNextOperationElement())
    {
      if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counter >= $FORCE_LIMIT)
      {
        break;
      }
      $this->counter++;
      $this->log(str_repeat("-", 80) . "[" . $this->counter . "]");

      //the default behaviour is to keep operation and increment attempt count on it
      $op = TriggeredOperation::TR_OP_INCREMENT;

      try
      {
        $operator = $this->getOperatorInstanceForOperationElement($operationElement);
        $operator->sync();
        $op = $operator->getTaskOnTrigger();
      } catch(OperatorNotFoundException $e)
      {
        $op = TriggeredOperation::TR_OP_DELETE;
        $this->log("No operator: " . $e->getMessage());
      } catch(\Exception $e)
      {
        $this->log($e->getMessage());
      }

      $this->executeTaskOnOperationElement($operationElement, $op);
    }
    $this->log("Executed #" . $this->counter . " (FORCE_LIMIT=$FORCE_LIMIT)");
  }

  /**
   * @param \stdClass $operationElement
   * @param int       $task
   */
  protected function executeTaskOnOperationElement(\stdClass $operationElement, $task)
  {
    if ($task == TriggeredOperation::TR_OP_INCREMENT
        && intval($operationElement->sync_attempt_count) >= $this->maxOperationFails
    )
    {
      $this->log(
        "Operation has reached maximum(" . $this->maxOperationFails . ") number of attempts! Setting to delete."
      );
      $task = TriggeredOperation::TR_OP_DELETE;
    }

    $taskName = 'UNKNOWN';
    switch ($task)
    {
      case TriggeredOperation::TR_OP_NOTHING:
        $taskName = 'NOTHING';
        //we update sync_datetime anyways so that other operations get a chance to be executed
        $sql = "UPDATE [Crm2Metodo].[dbo].[TriggeredOperations] SET sync_datetime = GETDATE()" . " WHERE id = "
               . $operationElement->id;
        break;
      case TriggeredOperation::TR_OP_INCREMENT:
        $taskName = 'INCREMENT';
        $sql = "UPDATE [Crm2Metodo].[dbo].[TriggeredOperations] SET sync_datetime = GETDATE(),"
               . " sync_attempt_count = " . (intval($operationElement->sync_attempt_count) + 1) . " WHERE id = "
               . $operationElement->id;
        break;
      case TriggeredOperation::TR_OP_DELETE:
        $taskName = 'DELETE';
        $sql = "DELETE FROM [Crm2Metodo].[dbo].[TriggeredOperations]" . " WHERE id = " . $operationElement->id;
        break;
    }
    if (isset($sql))
    {
      $this->log("Element Operation[$taskName]: " /*. $sql*/);
      try
      {
        $st = $this->db->prepare($sql);
        $st->execute();
      } catch(\Exception $e)
      {
        $this->log("SQL FAIL: " . $e->getMessage());
      }
    }
  }

  /**
   * @return mixed
   */
  protected function getNextOperationElement()
  {
    if (!$this->localItemStatement)
    {
      $db = Configuration::getDatabaseConnection("SERVER2K8");
      $sql = "SELECT * 
        FROM [Crm2Metodo].[dbo].[TriggeredOperations] AS T
        ORDER BY T.sync_datetime ASC, T.operation_datetime ASC
        ";
      $this->localItemStatement = $db->prepare($sql);
      $this->localItemStatement->execute();
    }
    $item = $this->localItemStatement->fetch(\PDO::FETCH_OBJ);
    if (!$item)
    {
      $this->localItemStatement = NULL;
    }
    else
    {
      foreach ($item as $k => &$itemData)
      {
        $itemData = trim($itemData);
        if ($k == 'operation_datetime')
        {
          $itemData = \DateTime::createFromFormat('Y-m-d H:i:s.u', $itemData);
        }
        if ($k == 'sync_datetime')
        {
          $itemData = \DateTime::createFromFormat('Y-m-d H:i:s.u', $itemData);
        }
      }
    }
    return $item;
  }

  /**
   * @param \stdClass $operationElement
   * @return TriggeredOperationInterface
   * @throws \Exception
   */
  protected function getOperatorInstanceForOperationElement($operationElement)
  {
    if (!isset($operationElement->table_name) || empty($operationElement->table_name))
    {
      throw new OperatorNotFoundException("Column 'table_name' is missing Operation Element");
    }
    $table_name = $operationElement->table_name;
    //$this->log("Looking for Operator for table: " . $table_name);

    if (!array_key_exists($table_name, $this->tableMap))
    {
      throw new OperatorNotFoundException("Table name($table_name) is not defined in table_map");
    }

    $tableMapItem = $this->tableMap[$table_name];
    //$this->log("Table Map Item: " . json_encode($tableMapItem));

    $operatorClassName = $tableMapItem["operation-class-name"];
    //$this->log("Found Operator class name: " . $operatorClassName);

    if ($operatorClassName == 'Mekit\Sync\TriggeredOperations\DocumentTypeSelector')
    {
      $selector = new DocumentTypeSelector($this->logger);
      $operatorClassName = $selector->getClassNameForDocTypeOperationElement($operationElement);
      //$this->log("Found Operator(DT) class name: " . $operatorClassName);
    }

    $this->checkOperationClass($operatorClassName);
    $reflection = new \ReflectionClass($operatorClassName);

    $operationElement->tableMapItem = $tableMapItem;
    /** @var TriggeredOperationInterface $operatorInstance */
    $operatorInstance = $reflection->newInstanceArgs([$this->logger, $operationElement]);

    return $operatorInstance;
  }

  /**
   * @throws \Exception
   */
  protected function setupTableMap()
  {
    $cfg = Configuration::getConfiguration();
    if (!isset($cfg["table-map"]))
    {
      throw new OperatorNotFoundException("Missing 'table-map' key from configuration file!");
    }
    if (!is_array($cfg["table-map"]))
    {
      throw new OperatorNotFoundException("The 'table-map' key in configuration must be an array!");
    }
    $tableMap = [];
    foreach ($cfg["table-map"] as $tableName => $operationClassName)
    {
      $tableNameParts = $this->getMSSQLTableNameParts($tableName);
      $this->checkMSSQLTable($tableNameParts);
      $tableMap[$tableName] = [
        'table-name-parts' => $tableNameParts,
        'operation-class-name' => $operationClassName,
      ];
    }
    //$this->log("TABLE MAP: " . json_encode($tableMap));

    $this->tableMap = $tableMap;
  }

  /**
   * @param string $operationClassName
   * @throws \Exception
   */
  protected function checkOperationClass($operationClassName)
  {
    if (!class_exists($operationClassName))
    {
      throw new OperatorNotFoundException("Inexistent operation class(" . $operationClassName . ")!");
    }
    $reflection = new \ReflectionClass($operationClassName);
    if (!$reflection->implementsInterface('Mekit\Sync\TriggeredOperations\TriggeredOperationInterface'))
    {
      throw new OperatorNotFoundException(
        "Operation class(" . $operationClassName . ") does not implement TriggeredOperationInterface!"
      );
    }
    if (!$reflection->isSubclassOf('Mekit\Sync\TriggeredOperations\TriggeredOperation'))
    {
      throw new OperatorNotFoundException(
        "Operation class(" . $operationClassName . ") does not extend TriggeredOperation!"
      );
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

  /**
   * @param string $msg
   * @param int    $level
   * @param array  $context
   */
  protected function log($msg, $level = Logger::INFO, $context = [])
  {
    $msg = "[OperationsEnumerator]" . $msg;
    call_user_func($this->logger, $msg, $level, $context);
  }
}