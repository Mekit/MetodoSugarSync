<?php
/**
 * Created by Adam Jakab.
 * Date: 12/07/16
 * Time: 10.03
 */

namespace Mekit\Sync\TriggeredOperations\Operators;

use Mekit\SugarCrm\Rest\v4_1\SugarCrmRestException;
use Mekit\Sync\TriggeredOperations\TriggeredOperation;
use Mekit\Sync\TriggeredOperations\TriggeredOperationInterface;

class Document extends TriggeredOperation implements TriggeredOperationInterface
{
  /** @var  string */
  protected $logPrefix = 'Document';

  protected $handledDocTypes = ['RAS'];

  /**
   * @return bool
   */
  public function sync()
  {
    $result = FALSE;
    $dataElement = $this->getDataElement();

    if (!$dataElement)
    {
      $this->setTaskOnTrigger(TriggeredOperation::TR_OP_DELETE);
      return $result;
    }

    if (!in_array($dataElement->TIPODOC, $this->handledDocTypes))
    {
      $this->setTaskOnTrigger(TriggeredOperation::TR_OP_DELETE);
      return $result;
    }

    if ($this->operationElement->operation_type == "D")
    {
      $result = $this->crmDeleteItem($dataElement);
    }
    else if (in_array($this->operationElement->operation_type, ['C', 'U']))
    {
      $result = $this->crmUpdateItem($dataElement);
    }

    if ($result)
    {
      //$this->setTaskOnTrigger(TriggeredOperation::TR_OP_DELETE);
    }
    else
    {
      //$this->setTaskOnTrigger(TriggeredOperation::TR_OP_INCREMENT);
    }


    return $result;
  }

  /**
   * @param \stdClass $dataElement
   * @return bool
   */
  protected function crmUpdateItem($dataElement)
  {
    $answer = FALSE;

    $dataElement = $this->updateCrmDataOnDataElement($dataElement);
    print_r($dataElement);

    $syncItem = $dataElement->crmData;

    $arguments = [
      'module_name' => 'Cases',
      'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($syncItem),
    ];

    try
    {
      $result = $this->sugarCrmRest->comunicate('set_entries', $arguments);
      $this->log("REMOTE RESULT: " . json_encode($result));

      $answer = TRUE;
    } catch(SugarCrmRestException $e)
    {
      //fail silently - we will do it next time
    }

    return $answer;
  }

  /**
   * @param \stdClass $dataElement
   * @return bool
   */
  protected function crmDeleteItem($dataElement)
  {
    $answer = FALSE;

    return $answer;
  }

  /**
   * @param \stdClass $dataElement
   * @return \stdClass
   */
  protected function updateCrmDataOnDataElement($dataElement)
  {
    $crmData = new \stdClass();

    $existingCaseId = $this->crmLoadRemoteCaseId($dataElement->PROGRESSIVO);

    $relatedAccount = $this->crmLoadRelatedAccount($dataElement->CODCLIFOR);

    $dataDoc = \DateTime::createFromFormat('Y-m-d H:i:s.u', $dataElement->DATADOC);
    $closeDataDoc = $dataDoc->add(new \DateInterval('P7D'))->format('Y-m-d');

    $crmData->id = $existingCaseId;

    $crmData->name = 'RAS #' . $dataElement->NUMERODOC . '/' . $dataElement->ESERCIZIO . ' - '
                     . $relatedAccount['name'];

    $crmData->type = 4;//Assistenza Tecnica

    $crmData->state = $dataElement->DOCCHIUSO == 1 ? 2 : 1;//1=Aperto / 2=Chiuso

    $crmData->status = $dataElement->DOCCHIUSO == 1 ? "2_1" : "1_1";//1_1=New / 2_2=Terminato

    $crmData->priority = 'P4';//Bassa

    $crmData->account_id = $relatedAccount['id'];

    $crmData->area_dinteresse_imp_c = 'service';//service = Assistenza

    //@todo: usare: [CODAGENTE1]
    $crmData->assigned_user_id = 'bbe923ec-d288-b3a3-5b0c-5370bd2b9e40';//Chiara Aragno

    $crmData->date_close_prg_c = $closeDataDoc;

    $crmData->imp_ras_number_c = $dataElement->NUMERODOC . '/' . $dataElement->ESERCIZIO;

    //@todo: prendere righe doc
    $crmData->n_matricola_macchinario_c = '';

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
   * @param string $CODCLIFOR
   * @return array
   * @throws SugarCrmRestException
   */
  protected function crmLoadRelatedAccount($CODCLIFOR)
  {
    $answer = [
      'id' => FALSE,
      'name' => $CODCLIFOR,
    ];
    $arguments = [
      'module_name' => 'Accounts',
      'query' => "",
      'order_by' => "",
      'offset' => 0,
      'select_fields' => ['id', 'name'],
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
        $this->log("FOUND REMOTE ITEM: " . json_encode($remoteItem));
        $answer['id'] = $remoteItem->id->value;
        $answer['name'] = $remoteItem->name->value;
      }
    }

    return $answer;
  }
}