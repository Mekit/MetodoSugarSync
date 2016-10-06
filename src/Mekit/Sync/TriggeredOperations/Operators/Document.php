<?php
/**
 * Created by Adam Jakab.
 * Date: 12/07/16
 * Time: 10.03
 */

namespace Mekit\Sync\TriggeredOperations\Operators;

use Mekit\Console\Configuration;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRestException;
use Mekit\Sync\TriggeredOperations\TriggeredOperation;
use Mekit\Sync\TriggeredOperations\TriggeredOperationInterface;

/**
 * This class for now is specific to RAS type documents - if you need other types
 * you will need to find a way to have type based classes
 *
 * Class Document
 * @package Mekit\Sync\TriggeredOperations\Operators
 */
class Document extends TriggeredOperation implements TriggeredOperationInterface
{
  /** @var  string */
  protected $logPrefix = 'Document';


  public function __construct(callable $logger, \stdClass $operationElement)
  {
    parent::__construct($logger, $operationElement);
  }

  /**
   * @return bool
   */
  public function sync()
  {
    $result = FALSE;

    if ($this->operationElement->operation_type == "D")
    {
      //data element has already been deleted - we only need identifier and document type
      $dataElement = new \stdClass();
      $dataElement->PROGRESSIVO = $this->operationElement->identifier_data;
      $dataElement->TIPODOC = $this->operationElement->param1;
    }
    else
    {
      $dataElement = $this->getDataElement();
    }

    $op = TriggeredOperation::TR_OP_DELETE;
    try
    {
      $result = $this->crmUpdateItem($dataElement);
      $this->log("UPDATE RESULT: " . ($result ? "SUCCESS" : "FAIL"));
      $op = ($result ? TriggeredOperation::TR_OP_DELETE : TriggeredOperation::TR_OP_INCREMENT);
    } catch(\Exception $e)
    {
      $this->log("ERROR: " . $e->getMessage());
    }

    $this->setTaskOnTrigger($op);

    return $result;
  }

  /**
   * Must be implemented in each docType class
   *
   * @param \stdClass $dataElement
   * @throws \Exception
   * @return bool
   */
  protected function crmUpdateItem($dataElement)
  {
    return FALSE;
  }

  /**
   * @param string    $moduleName
   * @param \stdClass $syncItem
   * @return bool
   */
  protected function crmSyncItem($moduleName, $syncItem)
  {
    $answer = FALSE;

    if (isset($syncItem->deleted) && $syncItem->deleted == 1 && !$syncItem->id)
    {
      $this->log("CANNOT DELETE ITEM WITHOUT ID - DELETE WILL BE SKIPPED");
      return $answer;
    }

    $arguments = [
      'module_name' => $moduleName,
      'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($syncItem),
    ];

    $this->log("SYNC ITEM: " . print_r($syncItem, TRUE));

    try
    {
      $this->sugarCrmRest->comunicate('set_entries', $arguments);
      $answer = TRUE;
    } catch(SugarCrmRestException $e)
    {
      //fail silently - we will do it next time
    }

    return $answer;
  }

  /**
   *
   * @todo: this is doggy: look in account - we need to bail out if multiple or if failure
   * otherwise we risk to create multiple/duplicated elements
   *
   * @param string $moduleName
   * @param string $query
   * @return bool|string
   * @throws SugarCrmRestException
   */
  protected function crmLoadRemoteIdForModule($moduleName, $query)
  {
    $crm_id = FALSE;
    $arguments = [
      'module_name' => $moduleName,
      'query' => $query,
      'order_by' => "",
      'offset' => 0,
      'select_fields' => ['id'],
      'link_name_to_fields_array' => [],
      'max_results' => 2,
      'deleted' => FALSE,
      'Favorites' => FALSE,
    ];

    /** @var \stdClass $result */
    $result = $this->sugarCrmRest->comunicate('get_entry_list', $arguments);

    if (isset($result) && isset($result->entry_list) && count($result->entry_list) == 1)
    {
      /** @var \stdClass $remoteItem */
      $remoteItem = $result->entry_list[0];
      $crm_id = $remoteItem->id;
    }

    return $crm_id;
  }

  /**
   * @param string $PROGRESSIVO
   * @return bool|array
   */
  protected function metodoLoadRelatedDocumentLines($PROGRESSIVO)
  {
    $db = Configuration::getDatabaseConnection("SERVER2K8");
    $sql = "SELECT TIPORIGA, CODART, DESCRIZIONEART, NRRIFPARTITA, RIFCOMMCLI FROM IMP.dbo.RIGHEDOCUMENTI WHERE"
           . " IDTESTA = " . $PROGRESSIVO . " ORDER BY POSIZIONE";
    try
    {
      $st = $db->prepare($sql);
      $st->execute();
      $answer = $st->fetchAll(\PDO::FETCH_OBJ);
    } catch(\Exception $e)
    {
      $answer = FALSE;
    }
    return $answer;
  }

}