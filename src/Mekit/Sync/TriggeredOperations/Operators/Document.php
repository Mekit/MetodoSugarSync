<?php
/**
 * Created by Adam Jakab.
 * Date: 12/07/16
 * Time: 10.03
 */

namespace Mekit\Sync\TriggeredOperations\Operators;

use Mekit\Console\Configuration;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRestException;
use Mekit\Sync\ConversionHelper;
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

  /** @var  string */
  protected $databaseName;

  /** @var array */
  protected $userIdCache = [];


  public function __construct(callable $logger, \stdClass $operationElement)
  {
    parent::__construct($logger, $operationElement);
    $this->databaseName = strtolower($operationElement->tableMapItem["table-name-parts"]["catalog"]);
  }

  /**
   * @return bool
   */
  public function sync()
  {
    $result = FALSE;

    //@todo: TEMPORARY!!!!
    /*
    if($this->operationElement->param1 != "RAS" && $this->operationElement->id != "11752") {
      return $result;
    }*/

    $this->log("OE: " . json_encode($this->operationElement));
    $this->log("DATABASE: " . $this->databaseName);

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
   * @param \stdClass $dataElement - is the line loaded from TESTEDOCUMENTI identified by PROGRESSIVO
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
   * @param bool      $returnComunicationResult
   * @return bool
   */
  protected function crmSyncItem($moduleName, $syncItem, $returnComunicationResult = FALSE)
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
      $res = $this->sugarCrmRest->comunicate('set_entries', $arguments);
      if ($returnComunicationResult === FALSE)
      {
        $answer = TRUE;
      }
      else
      {
        $answer = $res;
      }
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
    $dbname = strtoupper($this->databaseName);

    $sql = "SELECT 
      CONCAT(TD.PROGRESSIVO, '-', RD.IDRIGA) AS LINEID,
      RD.TIPORIGA, 
      RD.CODART, 
      RD.DESCRIZIONEART, 
      RD.NRRIFPARTITA, 
      RD.RIFCOMMCLI, 
      RD.POSIZIONE AS line_order,
      RD.CODART AS article_code,
      RD.DESCRIZIONEART AS article_description,
      RD.NUMLISTINO AS price_list_number,
      (CASE WHEN RD.TIPORIGA = 'V' THEN 0 ELSE RD.QTAPREZZO * TD.SEGNO END) AS quantity,
      RD.UMPREZZO AS measure_unit,
      RD.TOTNETTORIGAEURO * TD.SEGNO AS net_total,
      RD.PREZZOUNITNETTOEURO * TD.SEGNO AS net_unit,
      CASE WHEN NULLIF(RD.CODART, '') IS NOT NULL THEN
      (CASE WHEN RD.TIPORIGA = 'V' THEN 0 ELSE RD.QTAPREZZO * TD.SEGNO END) * ART.PREZZOEURO
      ELSE 0
      END AS net_total_listino_42,
      RD.DATAMODIFICA AS metodo_last_update_time
      FROM [${dbname}].[dbo].[RIGHEDOCUMENTI] AS RD
      INNER JOIN [${dbname}].[dbo].[TESTEDOCUMENTI] AS TD ON TD.PROGRESSIVO = RD.IDTESTA
      LEFT OUTER JOIN [${dbname}].[dbo].[LISTINIARTICOLI] AS ART ON RD.CODART = ART.CODART AND ART.NRLISTINO = 42      
      WHERE IDTESTA = " . $PROGRESSIVO . " ORDER BY POSIZIONE";

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

  /**
   * @param string $agentCode
   * @param string $database
   * @return string
   */
  protected function metodoLoadUserIdByAgentCode($agentCode, $database)
  {
    $crm_id = FALSE;
    $fieldName = ($database == 'imp' ? 'imp_agent_code_c' : 'mekit_agent_code_c');
    $agentCode = ConversionHelper::fixAgentCode($agentCode, ["A"], TRUE);
    if (!empty($agentCode))
    {
      if (isset($this->userIdCache[$database][$agentCode]) && !empty($this->userIdCache[$database][$agentCode]))
      {
        $crm_id = $this->userIdCache[$database][$agentCode];
      }
      else
      {
        $arguments = [
          'module_name' => 'Users',
          'query' => "users_cstm." . $fieldName . " = '" . $agentCode . "'",
          'order_by' => "",
          'offset' => 0,
          'select_fields' => ['id'],
          'link_name_to_fields_array' => [],
          'max_results' => 1,
          'deleted' => FALSE,
          'Favorites' => FALSE,
        ];
        $result = $this->sugarCrmRest->comunicate('get_entry_list', $arguments);
        if (isset($result) && isset($result->entry_list))
        {
          if (count($result->entry_list) == 1)
          {
            /** @var \stdClass $remoteItem */
            $remoteItem = $result->entry_list[0];
            $this->log("FOUND REMOTE USER: " . json_encode($remoteItem));
            $crm_id = $remoteItem->id;
            $this->userIdCache[$database][$agentCode] = $crm_id;
          }
          else
          {
            $this->log("NO REMOTE USER: " . json_encode($arguments));
          }
        }
        else
        {
          $this->log("NO REMOTE USER: " . json_encode($arguments));
        }
      }
    }
    return ($crm_id);

  }

}