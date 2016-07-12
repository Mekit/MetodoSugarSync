<?php
/**
 * Created by Adam Jakab.
 * Date: 12/07/16
 * Time: 11.09
 */

namespace Mekit\Sync\TriggeredOperations;

use Mekit\Console\Configuration;
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

  /** @var  \PDO */
  protected $db;

  /** @var  \PDOStatement */
  protected $localItemStatement;

  /** @var int */
  protected $maxIncrement = 10;

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
  }

  /**
   * @param array $options
   */
  public function execute($options)
  {
    $this->log("Executing with options: " . json_encode($options));

    $FORCE_LIMIT = 1;

    while ($operationElement = $this->getNextOperationElement())
    {
      if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counter >= $FORCE_LIMIT)
      {
        break;
      }
      $this->counter++;
      $this->log("OP[" . $this->counter . "]: " . json_encode($operationElement));
      $operator = $this->getOperatorInstanceForOperationElement($operationElement);
      $operator->sync();
      $this->executeTaskOnOperationElement($operationElement, $operator->getTaskOnTrigger());
    }
  }

  /**
   * @param \stdClass $operationElement
   * @param int       $task
   */
  protected function executeTaskOnOperationElement(\stdClass $operationElement, $task)
  {
    if ($task == TriggeredOperation::TR_OP_INCREMENT
        && intval($operationElement->sync_attempt_count) >= $this->maxIncrement
    )
    {
      $task = TriggeredOperation::TR_OP_DELETE;
    }

    switch ($task)
    {
      case TriggeredOperation::TR_OP_NOTHING:
        $this->log("NO OPERATION TASK");
        //we update sync_datetime anyways so that other operations get a chance to be executed
        $sql = "UPDATE [Crm2Metodo].[dbo].[TriggeredOperations] SET sync_datetime = GETDATE()" . " WHERE id = "
               . $operationElement->id;
        break;
      case TriggeredOperation::TR_OP_INCREMENT:
        $this->log("INCREMENT TASK");
        $sql = "UPDATE [Crm2Metodo].[dbo].[TriggeredOperations] SET sync_datetime = GETDATE(),"
               . " sync_attempt_count = " . (intval($operationElement->sync_attempt_count) + 1) . " WHERE id = "
               . $operationElement->id;
        break;
      case TriggeredOperation::TR_OP_DELETE:
        $this->log("DELETE TASK");
        $sql = "DELETE FROM [Crm2Metodo].[dbo].[TriggeredOperations]" . " WHERE id = " . $operationElement->id;
        break;
    }
    if (isset($sql))
    {
      $this->log("SQL: " . $sql);
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
      $sql = "SELECT * FROM [Crm2Metodo].[dbo].[TriggeredOperations] AS T ORDER BY T.sync_datetime ASC";
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
      throw new \Exception("Column 'table_name' is missing Operation Element");
    }
    $table_name = $operationElement->table_name;
    if (!array_key_exists($table_name, $this->tableMap))
    {
      throw new \Exception("Table name($table_name) is not defined in table_map");
    }
    $tableMapItem = $this->tableMap[$table_name];
    $reflection = new \ReflectionClass($tableMapItem["operation-class-name"]);
    $operatorInstance = $reflection->newInstanceArgs([$this->logger, $operationElement]);
    return $operatorInstance;
  }

  /**
   * @param array $tableMap
   */
  public function setTableMap($tableMap)
  {
    $this->tableMap = $tableMap;
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