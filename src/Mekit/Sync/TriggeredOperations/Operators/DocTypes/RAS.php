<?php
/**
 * Created by Adam Jakab.
 * Date: 05/10/16
 * Time: 15.16
 */

namespace Mekit\Sync\TriggeredOperations\Operators\DocTypes;

use Mekit\Console\Configuration;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRestException;
use Mekit\Sync\ConversionHelper;
use Mekit\Sync\TriggeredOperations\TriggeredOperation;
use Mekit\Sync\TriggeredOperations\TriggeredOperationInterface;

class RAS extends TriggeredOperation implements TriggeredOperationInterface
{
  /** @var  string */
  protected $logPrefix = 'Document[RAS]';

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
   * @param \stdClass $dataElement
   * @throws \Exception
   * @return bool
   */
  protected function crmUpdateItem($dataElement)
  {
    $answer = FALSE;

    if (!$dataElement)
    {
      throw new \Exception("No datElement! Maybe record has been deleted.");
    }

    if ($this->operationElement->operation_type != "D")
    {
      $dataElement = $this->updateCrmDataOnDataElement($dataElement);
      $syncItem = $dataElement->crmData;
    }
    else
    {
      $syncItem = new \stdClass();
      $syncItem->deleted = 1;
    }

    try
    {
      $syncItem->id = $this->crmLoadRemoteCaseId($dataElement->PROGRESSIVO);
    } catch(\Exception $e)
    {
      $this->log($e->getMessage());
      $this->log("CANNOT LOAD ID FROM CRM - UPDATE WILL BE SKIPPED");
      return $answer;
    }

    if (isset($syncItem->deleted) && $syncItem->deleted == 1 && !$syncItem->id)
    {
      $this->log("CANNOT DELETE ITEM WITHOUT ID - DELETE WILL BE SKIPPED");
      return $answer;
    }

    $arguments = [
      'module_name' => 'Cases',
      'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($syncItem),
    ];

    $this->log("SYNC: " . print_r($syncItem, TRUE));

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
   * @param \stdClass $dataElement
   * @return \stdClass
   */
  protected function updateCrmDataOnDataElement($dataElement)
  {
    $crmData = new \stdClass();

    $relatedAccountId = $this->crmLoadRelatedAccountId($dataElement->CODCLIFOR);

    $relatedAccountData = $this->crmLoadRelatedAccountData($relatedAccountId);
    //$this->log("ACCOUNT DATA($relatedAccountId): " . json_encode($relatedAccountData));

    $relatedDocumentLines = $this->metodoLoadRelatedDocumentLines($dataElement->PROGRESSIVO);


    $dataDoc = \DateTime::createFromFormat('Y-m-d H:i:s.u', $dataElement->DATADOC);
    $closeDataDoc = $dataDoc->add(new \DateInterval('P7D'))->format('Y-m-d');

    $crmData->name = 'RAS ' . $dataElement->ESERCIZIO . '/' . $dataElement->NUMERODOC;

    $crmData->state = $dataElement->DOCCHIUSO == 1 ? 2 : 1;//1=Aperto / 2=Chiuso

    $crmData->status = $dataElement->DOCCHIUSO == 1 ? "2_1" : "1_1";//1_1=New / 2_2=Terminato

    $crmData->priority = 'P4';//Bassa

    if ($relatedAccountId)
    {
      $crmData->account_id = $relatedAccountId;
    }

    if (isset($relatedAccountData["shipping_address_city"]))
    {
      $crmData->jjwg_maps_address_c = $relatedAccountData["shipping_address_city"];
    }

    $crmData->area_dinteresse_imp_c = 'service';//service = Assistenza

    $crmData->assigned_user_id = 'bbe923ec-d288-b3a3-5b0c-5370bd2b9e40';//Chiara Aragno

    $crmData->date_close_prg_c = $closeDataDoc;

    $crmData->imp_ras_number_c = $dataElement->ESERCIZIO . '/' . $dataElement->NUMERODOC;


    $description = [];
    if ($relatedDocumentLines && count($relatedDocumentLines))
    {

      foreach ($relatedDocumentLines as $relatedDocumentLine)
      {
        if (!isset($crmData->ref_part_number_c) && $relatedDocumentLine->CODART)
        {
          $crmData->ref_part_number_c = ConversionHelper::cleanupSuiteCRMFieldValue($relatedDocumentLine->CODART);
          if ($relatedDocumentLine->DESCRIZIONEART)
          {
            $crmData->ref_part_description_c = ConversionHelper::cleanupSuiteCRMFieldValue($relatedDocumentLine->DESCRIZIONEART);
          }
          if ($relatedDocumentLine->NRRIFPARTITA)
          {
            $crmData->ref_part_unique_number_c = ConversionHelper::cleanupSuiteCRMFieldValue($relatedDocumentLine->NRRIFPARTITA);
          }
        }
        //RIFCOMMCLI
        if (!isset($crmData->rif_commessa_code_c) && $relatedDocumentLine->RIFCOMMCLI)
        {
          $crmData->rif_commessa_code_c = ConversionHelper::cleanupSuiteCRMFieldValue($relatedDocumentLine->RIFCOMMCLI);
        }
        //AGGIU
        if ($relatedDocumentLine->DESCRIZIONEART && !$relatedDocumentLine->CODART)
        {
          $description[] = ConversionHelper::cleanupSuiteCRMFieldValue($relatedDocumentLine->DESCRIZIONEART);
        }
      }
    }

    $crmData->type = 4;//4 = Assistenza Tecnica
    if (isset($crmData->rif_commessa_code_c))
    {
      if (preg_match('#^CTR#', $crmData->rif_commessa_code_c))
      {
        $crmData->type = 5;//5 = Assitenza Programmata
      }
    }

    //
    if (count($description))
    {
      $crmData->descrizione_problematica_c = implode("\n", $description);
    }

    $crmData->imp_doc_progressivo_c = $dataElement->PROGRESSIVO;

    $dataElement->crmData = $crmData;
    return $dataElement;
  }


  /**
   * @param string $PROGRESSIVO
   * @return bool|string
   * @throws SugarCrmRestException
   */
  protected function crmLoadRemoteCaseId($PROGRESSIVO)
  {
    $crm_id = FALSE;
    $arguments = [
      'module_name' => 'Cases',
      'query' => "cases_cstm.imp_doc_progressivo_c = '" . $PROGRESSIVO . "'",
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
   * @param string $crmid
   * @return array
   */
  protected function crmLoadRelatedAccountData($crmid)
  {
    $data = [];

    if ($crmid)
    {
      $arguments = [
        'module_name' => 'Accounts',
        'id' => $crmid,
        'select_fields' => ['id', 'shipping_address_city'],
        'link_name_to_fields_array' => [],
      ];

      try
      {
        /** @var \stdClass $result */
        $result = $this->sugarCrmRest->comunicate('get_entry', $arguments);
      } catch(SugarCrmRestException $e)
      {
        //bugger
      }


      if (isset($result) && isset($result->entry_list) && count($result->entry_list) == 1)
      {
        //$this->log("ACCOUNT DATA RES: " . json_encode($result));
        /** @var \stdClass $remoteItem */
        $remoteItem = $result->entry_list[0]->name_value_list;
        foreach ($arguments['select_fields'] as $fieldName)
        {
          if (isset($remoteItem->$fieldName->value))
          {
            $data[$fieldName] = $remoteItem->$fieldName->value;
          }
        }
        //$this->log("FOUND REMOTE ACCOUNT: " . json_encode($remoteItem));
        //$crm_id = $remoteItem->id->value;
      }
    }

    return $data;
  }

  /**
   * @param string $CODCLIFOR
   * @return string
   * @throws SugarCrmRestException
   */
  protected function crmLoadRelatedAccountId($CODCLIFOR)
  {
    $crm_id = FALSE;

    $arguments = [
      'module_name' => 'Accounts',
      'query' => "",
      'order_by' => "",
      'offset' => 0,
      'select_fields' => ['id'],
      'link_name_to_fields_array' => [],
      'max_results' => 2,
      'deleted' => FALSE,
      'Favorites' => FALSE,
    ];

    $codeFieldName = FALSE;
    if (strtoupper(substr($CODCLIFOR, 0, 1)) == "C")
    {
      $codeFieldName = 'imp_metodo_client_code_c';
    }
    else if (strtoupper(substr($CODCLIFOR, 0, 1)) == "F")
    {
      $codeFieldName = 'imp_metodo_supplier_code_c';
    }

    if ($codeFieldName)
    {
      $codeFieldValue = $CODCLIFOR;
      $arguments['query'] = "accounts_cstm." . $codeFieldName . " = '" . $codeFieldValue . "'";


      try
      {
        /** @var \stdClass $result */
        $result = $this->sugarCrmRest->comunicate('get_entry_list', $arguments);
      } catch(SugarCrmRestException $e)
      {
        //bugger
      }


      if (isset($result) && isset($result->entry_list) && count($result->entry_list) == 1)
      {
        /** @var \stdClass $remoteItem */
        $remoteItem = $result->entry_list[0]->name_value_list;
        //$this->log("FOUND REMOTE ACCOUNT: " . json_encode($remoteItem));
        $crm_id = $remoteItem->id->value;
      }
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