<?php
/**
 * Created by Adam Jakab.
 * Date: 05/10/16
 * Time: 15.16
 */

namespace Mekit\Sync\TriggeredOperations\Operators\DocTypes;

use Mekit\Console\Configuration;
use Mekit\DbCache\ProductCache;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRestException;
use Mekit\Sync\ConversionHelper;
use Mekit\Sync\TriggeredOperations\Operators\Document;

/**
 * Class OFC
 * @package Mekit\Sync\TriggeredOperations\Operators\DocTypes
 */
class OFC extends Document
{
  /** @var  string */
  protected $logPrefix = 'Document[OFC]';

  /** @var ProductCache */
  protected $productCache;

  /** @var array */
  protected $product_codes_vat_10 = ['FESEF002', 'PHF046', 'PHAC75', 'PHF004'];

  /**
   * OFC constructor.
   * @param callable  $logger
   * @param \stdClass $operationElement
   */
  public function __construct(callable $logger, \stdClass $operationElement)
  {
    parent::__construct($logger, $operationElement);
    $this->productCache = new ProductCache('Products', $logger);
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

    $dataElement = $this->updateCrmDataOnDataElement($dataElement);
    $syncItem = $dataElement->crmData;

    if ($this->operationElement->operation_type == "D")
    {
      $syncItem->deleted = 1;
      $answer = $this->crmSyncItem("AOS_Quotes", $syncItem);
      //is there anything else after delete to do?
    }
    else
    {
      $ofcSyncRes = $this->crmSyncItem("AOS_Quotes", $syncItem, TRUE);

      //only do the following operations if we are sure that quote has been created(has id)
      if ($ofcSyncRes && isset($ofcSyncRes->ids) && count($ofcSyncRes->ids))
      {
        $syncItem->id = $ofcSyncRes->ids[0];

        //RELATE TO ACCOUNT
        $remoteAccountId = FALSE;
        $res = $this->relateAccountToOffer($syncItem->id, $dataElement->CODCLIFOR);
        if ($res && isset($res->ids[0]) && !empty($res->ids[0]))
        {
          $remoteAccountId = $res->ids[0];
        }

        //RELATE TO OPPORTUNITY
        $remoteOpportunityId = FALSE;
        $res = $this->creareRelateOpportunityToOffer($dataElement, $remoteAccountId);
        if ($res && isset($res->ids[0]) && !empty($res->ids[0]))
        {
          //we can have true(wgen skipped because of date) / false as well
          $remoteOpportunityId = $res->ids[0];
        }

        // SAVE LINES Of OFFER
        //$this->saveOfferLinesOnRemote();

        $answer = TRUE;
      }
    }

    //$answer = $this->crmSyncItem("AOS_Quotes", $syncItem);
    return $answer;
  }

  /**
   * @param \stdClass $dataElement
   * @return \stdClass
   */
  protected function updateCrmDataOnDataElement($dataElement)
  {
    $restOperation = "INSERT";
    $crmData = new \stdClass();

    //dataElement now holds ONLY TESTEDOCUMENTI - adding some more stuff
    $dataElement = $this->addMetodoData_TABPAGAMENTI($dataElement);
    $dataElement = $this->addMetodoData_EXTRATESTEDOC($dataElement);
    $dataElement->lines = $this->metodoLoadRelatedDocumentLines($dataElement->PROGRESSIVO);


    // ID
    $crm_id = $this->crmLoadRemoteIdForModule(
      "AOS_Quotes", "aos_quotes_cstm.metodo_id_head_c = '" . $dataElement->PROGRESSIVO . "'"
                    . " AND aos_quotes_cstm.metodo_database_c = '" . $this->databaseName . "'"
    );
    //add id to sync item for update

    if ($crm_id)
    {
      $crmData->id = $crm_id;
      $restOperation = "UPDATE";
    }

    if ($this->operationElement->operation_type != "D")
    {
      // Assigned User ID
      $assignedUserId = $this->metodoLoadUserIdByAgentCode($dataElement->CODAGENTE1, $this->databaseName);
      $crmData->assigned_user_id = ($assignedUserId ? $assignedUserId : 1);//assign to administrator if no user found

      //modify sync item here
      $crmData->metodo_database_c = $this->databaseName;


      $crmData->metodo_id_head_c = $dataElement->PROGRESSIVO;
      $crmData->document_number_c = $dataElement->NUMERODOC;
      $crmData->assigned_user_id = $assignedUserId;

      $dataDoc = \DateTime::createFromFormat('Y-m-d H:i:s.u', $dataElement->DATADOC);
      $crmData->data_doc_c = $dataDoc->format("Y-m-d");
      $crmData->expiration = $dataDoc->format("Y-m-d");
      $crmData->document_year_c = $dataDoc->format("Y");

      $crmData->name = strtoupper($this->databaseName) . ' - ' . $dataDoc->format("Y") . '/' . $dataElement->NUMERODOC;

      $crmData->stage = 'Draft';
      $crmData->approval_status = 'Approved';
      $crmData->currency_id = '-99';//EUR

      $agentCode = ConversionHelper::fixAgentCode($dataElement->CODAGENTE1, ['A'], TRUE);
      $crmData->imp_agent_code_c = ($this->databaseName == 'imp' ? $agentCode : '');
      $crmData->mekit_agent_code_c = ($this->databaseName == 'mekit' ? $agentCode : '');

      //from TABPAGAMENTI
      $crmData->dsc_payment_c = isset($dataElement->TABPAGAMENTI->DESCRIZIONE) ? $dataElement->TABPAGAMENTI->DESCRIZIONE : '';

      $groupData = $this->createGroupDataFromOfferLines($dataElement->lines, NULL, NULL);

      //TOTALE BEFORE DISCOUNT
      $crmData->total_amt = $groupData->total_amt;
      $crmData->total_amt_usdollar = $groupData->total_amt_usdollar;

      //TOTALE AFTER DISCOUNT
      $crmData->subtotal_amount = $groupData->subtotal_amount;
      $crmData->subtotal_amount_usdollar = $groupData->subtotal_amount_usdollar;

      //TOTALE DISCOUNT
      $crmData->discount_amount = $groupData->discount_amount;
      $crmData->discount_amount_usdollar = $groupData->discount_amount_usdollar;

      //TOTALE DISCOUNT(%)
      $crmData->discount_percent_c = $groupData->discount_percent;

      //TOTALE TAX
      $crmData->tax_amount = $groupData->tax_amount;
      $crmData->tax_amount_usdollar = $groupData->tax_amount_usdollar;

      //SHIPPING
      $crmData->shipping_amount = 0;
      $crmData->shipping_amount_usdollar = 0;
      $crmData->shipping_tax = 0;
      $crmData->shipping_tax_amt = 0;
      $crmData->shipping_tax_amt_usdollar = 0;

      //TOTALE DOCUMENTO
      $crmData->total_amount = $groupData->total_amount;
      $crmData->total_amount_usdollar = $groupData->total_amount_usdollar;
    }

    $this->log("DATA ELEMENT[$restOperation]: " . print_r($dataElement, TRUE));

    $this->log("CRMDATA: " . print_r($crmData, TRUE));

    $dataElement->crmData = $crmData;
    return $dataElement;
  }


  /**
   * @param array  $offerLines
   * @param string $crm_offer_id
   * @param string $crm_line_group_id
   * @return \stdClass
   */
  protected function createGroupDataFromOfferLines($offerLines, $crm_offer_id, $crm_line_group_id)
  {
    $answer = new \stdClass();

    if (!empty($crm_line_group_id))
    {
      $answer->id = $crm_line_group_id;
    }
    $answer->name = 'RIGHE DA METODO';
    $answer->assigned_user_id = '1';
    $answer->number = 1;
    $answer->currency_id = '-99';
    $answer->parent_type = 'AOS_Quotes';
    $answer->parent_id = $crm_offer_id;

    $netTotal = 0;
    $netTotal42 = 0;
    $discountTotal = 0;
    $taxTotal = 0;

    /** @var \stdClass $offerLine */
    foreach ($offerLines as $offerLine)
    {
      $calc = $this->getCalculatedValuesForOfferLine($offerLine);
      $netTotal += $calc['product_total_price'];
      $netTotal42 += $calc['product_list_price'];
      $discountTotal += $calc['product_discount_amount'];
      $taxTotal += $calc['vat_amt'];
    }
    $grossTotal = $netTotal + $taxTotal;

    $discountPercent = 0;
    if ($netTotal42 != 0)
    {
      $discountPercent = (100 * (1 - ($netTotal / $netTotal42)));
    }


    //Totale - Prezzo Da Listino 42
    $answer->total_amt = ConversionHelper::fixCurrency($netTotal42);
    $answer->total_amt_usdollar = ConversionHelper::fixCurrency($netTotal42);

    //Sconto
    $answer->discount_amount = ConversionHelper::fixCurrency($discountTotal);
    $answer->discount_amount_usdollar = ConversionHelper::fixCurrency($discountTotal);
    $answer->discount_percent = ConversionHelper::fixCurrency($discountPercent, 1);

    //Imponibile
    $answer->subtotal_amount = ConversionHelper::fixCurrency($netTotal);
    $answer->subtotal_amount_usdollar = ConversionHelper::fixCurrency($netTotal);

    //Tasse
    $answer->tax_amount = ConversionHelper::fixCurrency($taxTotal);
    $answer->tax_amount_usdollar = ConversionHelper::fixCurrency($taxTotal);

    //ALWAYS ZERO!
    $answer->subtotal_tax_amount = 0;
    $answer->subtotal_tax_amount_usdollar = 0;

    //Totale IVATO
    $answer->total_amount = ConversionHelper::fixCurrency($grossTotal);
    $answer->total_amount_usdollar = ConversionHelper::fixCurrency($grossTotal);

    return $answer;
  }

  /**
   * @param \stdClass $offerLine
   * @return array
   */
  protected function getCalculatedValuesForOfferLine($offerLine)
  {
    $answer = [
      'product_crm_id' => NULL,
      'product_qty' => 0,
      'product_cost_price' => 0,
      'product_list_price' => 0,
      'product_list_price_unit' => 0,
      'discount_type' => 'Percentage',
      'product_discount' => 0,
      'product_discount_amount' => 0,
      'product_discount_amount_unit' => 0,
      'product_unit_price' => 0,
      'vat' => '22.0',
      'vat_amt' => 0,
      'product_total_price' => 0
    ];

    //FIND PRODUCT
    $relatedProduct = FALSE;
    if (!empty($offerLine->article_code))
    {
      $candidates = $this->productCache->loadItems(['id' => $offerLine->article_code]);
      //there should be ONLY ONE
      if (isset($candidates[0]))
      {
        $relatedProduct = $candidates[0];
        if (!empty($relatedProduct->crm_id))
        {
          $answer['product_crm_id'] = $relatedProduct->crm_id;
        }
      }
    }

    //GENERIC LOCAL VALUES
    $quantity = (float) $offerLine->quantity;
    $net_unit = !empty($offerLine->net_unit) ? (float) $offerLine->net_unit : 0;
    $net_total = !empty($offerLine->net_total) ? (float) $offerLine->net_total : 0;
    $net_total_l42 = !empty($offerLine->net_total_listino_42) ? (float) $offerLine->net_total_listino_42 : 0;
    $discount_total = $net_total_l42 - $net_total;
    $discount_percent = 0;
    if ($net_total_l42 != 0)
    {
      $discount_percent = (100 * (1 - ($net_total / $net_total_l42)));
    }
    $tax_rate = (in_array($offerLine->article_code, $this->product_codes_vat_10) ? 10 : 22);
    $tax_multiplier = $tax_rate / 100;
    $taxTotal = $net_total * $tax_multiplier;

    //QUANTITY
    $answer['product_qty'] = $quantity;

    //Costo Prodotto
    if ($relatedProduct)
    {
      $product_cost = !empty($relatedProduct->cost) ? (float) $relatedProduct->cost : 0;
      $answer['product_cost_price'] = $product_cost;
    }

    //Prezzo Prodotto - L42
    $answer['product_list_price'] = $net_total_l42;
    if ($quantity != 0)
    {
      $answer['product_list_price_unit'] = ($net_total_l42 / $quantity);
    }
    else
    {
      $answer['product_list_price_unit'] = 0;
    }

    //SCONTO
    $answer['product_discount'] = $discount_percent;
    $answer['product_discount_amount'] = (0 - $discount_total);//this must be negative
    if ($quantity != 0)
    {
      $answer['product_discount_amount_unit'] = (0 - ($discount_total / $quantity));
    }
    else
    {
      $answer['product_discount_amount_unit'] = 0;
    }

    //Product Unit price
    $answer['product_unit_price'] = $net_unit;

    //TAX
    $answer['vat'] = $tax_rate . '.0';//this is dropdown and needs '22.0' or '10.0'
    $answer['vat_amt'] = $taxTotal;

    //TOTAL PRICE
    $answer['product_total_price'] = $net_total;

    return $answer;
  }

  /**
   * create/relate to Opportunity
   *
   * @param \stdClass $dataElement
   * @param string    $remoteAccountId
   * @return bool
   * @throws \Exception
   */
  protected function creareRelateOpportunityToOffer($dataElement, $remoteAccountId)
  {
    $remoteOpportunityItem = FALSE;
    $skipBecauseOfError = FALSE;
    $dataDocumento = \DateTime::createFromFormat('Y-m-d H:i:s.u', $dataElement->DATADOC);
    $fixedDate = \DateTime::createFromFormat("Y-m-d", "2015-09-01");//do not create opportunities before this date
    if ($dataDocumento->format('U') >= $fixedDate->format('U'))
    {
      $opportunity_id = FALSE;
      try
      {
        $opportunity_id = $this->loadRemoteOpportunityId(
          $dataElement->crmData->id, $dataElement->EXTRATESTEDOC->RifOffertaCRM
        );
      } catch(SugarCrmRestException $e)
      {
        $skipBecauseOfError = TRUE;
        //keep quiet
      }
      if (!$skipBecauseOfError)
      {
        if ($opportunity_id === FALSE)
        {
          $restOperation = "INSERT";
          $syncItem = new \stdClass();

          $dataDoc = \DateTime::createFromFormat('Y-m-d H:i:s.u', $dataElement->DATADOC);

          $syncItem->name = $dataElement->NUMRIFDOC;
          $syncItem->date_closed = $dataDoc->add(new \DateInterval('P10D'))->format('Y-m-d');
          $syncItem->statovendita_c = '2';//Offerta
          $syncItem->amount = ConversionHelper::fixCurrency($dataElement->TOTIMPONIBILEEURO);
          $syncItem->amount_usdollar = ConversionHelper::fixCurrency($dataElement->TOTIMPONIBILEEURO);
          $syncItem->assigned_user_id = $dataElement->crmData->assigned_user_id;
          $syncItem->account_id = $remoteAccountId;

          //create arguments for rest
          $arguments = [
            'module_name' => 'Opportunities',
            'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($syncItem),
          ];

          $this->log("CRM SYNC OPPORTUNITIES[" . $restOperation . "]: " . json_encode($arguments));

          try
          {
            $remoteOpportunityItem = $this->sugarCrmRest->comunicate('set_entries', $arguments);
            $this->log("REMOTE RESULT: " . json_encode($remoteOpportunityItem));
            if (isset($remoteOpportunityItem->ids[0]) && !empty($remoteOpportunityItem->ids[0]))
            {
              $opportunity_id = $remoteOpportunityItem->ids[0];
            }
          } catch(SugarCrmRestException $e)
          {
            //go ahead with false silently
            $this->log("REMOTE ERROR!!! - " . $e->getMessage());
          }
        }

        //RELATE OPPORTUNITY TO OFFER
        if ($opportunity_id)
        {
          $arguments = [
            'module_name' => 'AOS_Quotes',
            'module_id' => $dataElement->crmData->id,
            'link_field_name' => 'opportunities',
            'related_ids' => [$opportunity_id]
          ];
          try
          {
            $result = $this->sugarCrmRest->comunicate('set_relationship', $arguments);
            $this->log("RELATIONSHIP RESULT: " . json_encode($result));
            if (!isset($result->created) || $result->created != 1)
            {
              $this->log("OPPORTUNITY RELATIONSHIP ERROR!!! - " . json_encode($arguments));
              $remoteOpportunityItem->ids[0] = $opportunity_id;
            }
            else
            {
              $this->log("OPPORTUNITY RELATIONSHIP CREATED!!! - " . json_encode($result));
              $remoteOpportunityItem = TRUE;//so that cache item is still saved
            }
          } catch(SugarCrmRestException $e)
          {
            //go ahead with false silently
            $this->log(
              "OPPORTUNITY REMOTE ERROR!!! - " . $e->getMessage() . " - " . json_encode($arguments)
            );
            $remoteOpportunityItem = FALSE;
          }
        }
      }
    }
    else
    {
      $this->log("Skipping Opportunity creation for offer older than: " . $fixedDate->format("Y-m-d"));
      $remoteOpportunityItem = TRUE;//so that cache item is still saved
    }
    return $remoteOpportunityItem;
  }

  /**
   * @param string $offerId
   * @param string $codCliFor - codice cliente/fornitore
   * @return \stdClass|bool
   * @throws \Exception
   */
  protected function relateAccountToOffer($offerId, $codCliFor)
  {
    $result = FALSE;

    //identify by codice metodo - the first one found
    $remoteFieldName = $this->getRemoteFieldNameForCodiceMetodo($this->databaseName, $codCliFor);
    $account_id = $this->crmLoadRemoteIdForModule(
      'Accounts', "accounts_cstm." . $remoteFieldName . " = '" . $codCliFor . "'"
    );

    if ($account_id)
    {
      $this->log("Relating Offer(" . $offerId . ") to Account: " . $account_id);
      $arguments = [
        'module_name' => 'Accounts',
        'module_id' => $account_id,
        'link_field_name' => 'aos_quotes',
        'related_ids' => [$offerId]
      ];
      try
      {
        $result = $this->sugarCrmRest->comunicate('set_relationship', $arguments);
        //$this->log("ACCOUNT RELATIONSHIP RESULT: " . json_encode($result));
        if (!isset($result->created) || $result->created != 1)
        {
          $this->log("ACCOUNT RELATIONSHIP ERROR!!! - " . json_encode($arguments));
        }
        else
        {
          $result = new \stdClass();
          $result->ids = [$account_id];
        }
      } catch(SugarCrmRestException $e)
      {
        //go ahead with false silently
        $this->log("REMOTE ERROR!!! - " . $e->getMessage() . " - " . json_encode($arguments));
      }
    }
    return $result;
  }

  /**
   * if 'rif_opportunity_identifier' is set we need to search directly the opportunities filtering by that code
   * otherwise we will check if we already have an opportunity related to this offer
   *
   * @param string $remoteOfferId
   * @param string $rif_opportunity_identifier
   * @return string
   */
  protected function loadRemoteOpportunityId($remoteOfferId, $rif_opportunity_identifier)
  {
    $crm_id = FALSE;
    if (!empty($rif_opportunity_identifier))
    {
      $arguments = [
        'module_name' => 'Opportunities',
        'query' => "opportunities_cstm.identificativo_c" . " = '" . $rif_opportunity_identifier . "'",
        'order_by' => "",
        'offset' => 0,
        'select_fields' => ['id'],
        'link_name_to_fields_array' => [],
        'max_results' => 1,
        'deleted' => FALSE,
        'Favorites' => FALSE,
      ];
      $result = $this->sugarCrmRest->comunicate('get_entry_list', $arguments);
      if (isset($result->entry_list) && count($result->entry_list) == 1)
      {
        /** @var \stdClass $remoteItem */
        $remoteItem = $result->entry_list[0];
        $this->log(
          "FOUND REMOTE OPPORTUNITY(BY REFID(" . $rif_opportunity_identifier . ")): " . json_encode($remoteItem)
        );
        $crm_id = $remoteItem->id;
      }
      else
      {
        $this->log("NO REMOTE OPPORTUNITY: " . json_encode($arguments));
      }
    }
    else
    {
      $arguments = [
        'module_name' => 'AOS_Quotes',
        'ids' => [$remoteOfferId],
        'select_fields' => ['id', 'name'],
        'link_name_to_fields_array' => [
          ['name' => 'opportunities', 'value' => ['id', 'name']]
        ],
      ];
      $result = $this->sugarCrmRest->comunicate('get_entries', $arguments);
      if (isset($result->relationship_list[0]))
      {
        $rel = $this->sugarCrmRest->getNameValueListFromEntyListItem(NULL, $result->relationship_list[0]);
        if (isset($rel->opportunities) && is_array($rel->opportunities) && count($rel->opportunities))
        {
          $relatedOpportunity = $rel->opportunities[0];
          $this->log("FOUND RELATED OPPORTUNITY: " . json_encode($relatedOpportunity));
          $crm_id = $relatedOpportunity->id;
        }
      }
    }

    return ($crm_id);
  }


  /**
   * @param \stdClass $dataElement
   * @return \stdClass
   */
  protected function addMetodoData_EXTRATESTEDOC($dataElement)
  {
    $db = Configuration::getDatabaseConnection("SERVER2K8");
    $sql = "SELECT * FROM [" . $this->databaseName . "].[dbo].[EXTRATESTEDOC] AS ET WHERE" . " ET.IDTESTA = '"
           . $dataElement->PROGRESSIVO . "'";
    try
    {
      $st = $db->prepare($sql);
      $st->execute();
      $dataElement->EXTRATESTEDOC = $st->fetch(\PDO::FETCH_OBJ);
    } catch(\Exception $e)
    {
      $dataElement->EXTRATESTEDOC = FALSE;
    }

    return $dataElement;
  }

  /**
   * @param \stdClass $dataElement
   * @return \stdClass
   */
  protected function addMetodoData_TABPAGAMENTI($dataElement)
  {
    $db = Configuration::getDatabaseConnection("SERVER2K8");
    $sql = "SELECT * FROM [" . $this->databaseName . "].[dbo].[TABPAGAMENTI] AS TP WHERE" . " TP.CODICE = '"
           . $dataElement->CODPAGAMENTO . "'";
    try
    {
      $st = $db->prepare($sql);
      $st->execute();
      $dataElement->TABPAGAMENTI = $st->fetch(\PDO::FETCH_OBJ);
    } catch(\Exception $e)
    {
      $dataElement->TABPAGAMENTI = FALSE;
    }

    return $dataElement;
  }

  //-------------------------------------------------------------------------------------------------------------UTILS
  /**
   * @param string $database
   * @param string $codice
   * @return string
   * @throws \Exception
   */
  protected function getRemoteFieldNameForCodiceMetodo($database, $codice)
  {
    $database = strtoupper($database);
    $type = strtoupper(substr($codice, 0, 1));
    switch ($database)
    {
      case "IMP":
        switch ($type)
        {
          case "C":
            $answer = "imp_metodo_client_code_c";
            break;
          case "F":
            $answer = "imp_metodo_supplier_code_c";
            break;
          default:
            throw new \Exception("Local item needs to have Tipologia C|F!");
        }
        break;
      case "MEKIT":
        switch ($type)
        {
          case "C":
            $answer = "mekit_metodo_client_code_c";
            break;
          case "F":
            $answer = "mekit_metodo_supplier_code_c";
            break;
          default:
            throw new \Exception("Local item needs to have Tipologia C|F!");
        }
        break;
      default:
        throw new \Exception("Local item needs to have database IMP|MEKIT!");
    }
    return $answer;
  }
}