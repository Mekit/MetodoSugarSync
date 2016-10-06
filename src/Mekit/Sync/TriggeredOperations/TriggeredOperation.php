<?php
/**
 * Created by Adam Jakab.
 * Date: 12/07/16
 * Time: 10.04
 */

namespace Mekit\Sync\TriggeredOperations;


use Mekit\Console\Configuration;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRest;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRestException;
use Monolog\Logger;

class TriggeredOperation
{
  const TR_OP_NOTHING = 0;
  const TR_OP_INCREMENT = 1;
  const TR_OP_DELETE = 2;

  /** @var callable */
  protected $logger;

  /** @var string */
  protected $logPrefix = '';

  /** @var  \stdClass */
  protected $operationElement;

  /** @var int */
  private $taskOnTrigger = self::TR_OP_NOTHING;

  /** @var SugarCrmRest */
  protected $sugarCrmRest;

  public function __construct(callable $logger, \stdClass $operationElement)
  {
    $this->logger = $logger;
    $this->operationElement = $operationElement;
    $this->sugarCrmRest = new SugarCrmRest();
  }

  /**
   * Loads the row from the database which is referenced by the operation element
   * through identifier column(id column name) and data(id of the column)
   *
   * @return bool|\stdClass
   */
  protected function getDataElement()
  {
    $db = Configuration::getDatabaseConnection("SERVER2K8");
    $sql = "SELECT * FROM " . $this->operationElement->table_name . " WHERE "
           . $this->operationElement->identifier_column . " LIKE " . $this->operationElement->identifier_data;
    try
    {
      $st = $db->prepare($sql);
      $st->execute();
      $dataElement = $st->fetch(\PDO::FETCH_OBJ);
    } catch(\Exception $e)
    {
      $dataElement = FALSE;
    }
    return $dataElement;
  }

  /**
   * @param int $taskOnTrigger
   */
  protected function setTaskOnTrigger($taskOnTrigger)
  {
    if ($taskOnTrigger != self::TR_OP_NOTHING && $taskOnTrigger != self::TR_OP_INCREMENT
        && $taskOnTrigger != self::TR_OP_DELETE
    )
    {
      $taskOnTrigger = self::TR_OP_NOTHING;
    }
    $this->taskOnTrigger = $taskOnTrigger;
  }

  /**
   * @return int
   */
  public function getTaskOnTrigger()
  {
    return $this->taskOnTrigger;
  }

  /**
   * @param string $msg
   * @param int    $level
   * @param array  $context
   */
  protected function log($msg, $level = Logger::INFO, $context = [])
  {
    $msg = "[" . $this->logPrefix . "]" . $msg;
    call_user_func($this->logger, $msg, $level, $context);
  }
}