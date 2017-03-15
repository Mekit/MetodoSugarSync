<?php
/**
 * Created by Adam Jakab.
 * Date: 01/12/15
 * Time: 11.32
 */

namespace Mekit\Sync\MetodoToCrm;

use Mekit\Console\Configuration;
use Mekit\DbCache\AccountCache;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRest;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRestException;
use Mekit\Sync\ConversionHelper;
use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;
use Monolog\Logger;

class AccountData extends Sync implements SyncInterface
{
  /** @var callable */
  protected $logger;

  /** @var SugarCrmRest */
  protected $sugarCrmRest;

  /** @var  AccountCache */
  protected $cacheDb;

  /** @var  \PDOStatement */
  protected $localItemStatement;

  /** @var array */
  protected $counters = [];

  /**
   * @param callable $logger
   */
  public function __construct($logger)
  {
    parent::__construct($logger);
    $this->cacheDb = new AccountCache('Account', $logger);
    $this->sugarCrmRest = new SugarCrmRest();
  }

  /**
   * @param array $options
   */
  public function execute($options)
  {
    //$this->log("EXECUTING..." . json_encode($options));
    if (isset($options["delete-cache"]) && $options["delete-cache"])
    {
      $this->cacheDb->removeAll();
    }

    if (isset($options["invalidate-cache"]) && $options["invalidate-cache"])
    {
      $this->cacheDb->invalidateAll(TRUE, TRUE);
    }

    if (isset($options["invalidate-local-cache"]) && $options["invalidate-local-cache"])
    {
      $this->cacheDb->invalidateAll(TRUE, FALSE);
    }

    if (isset($options["invalidate-remote-cache"]) && $options["invalidate-remote-cache"])
    {
      $this->cacheDb->invalidateAll(FALSE, TRUE);
    }

    if (isset($options["update-cache"]) && $options["update-cache"])
    {
      $this->updateLocalCache();
    }

    if (isset($options["update-remote"]) && $options["update-remote"])
    {
      $this->updateRemoteFromCache();
    }
  }

  protected function updateLocalCache()
  {
    $this->log("updating local cache...");
    $this->counters["cache"]["index"] = 0;
    foreach (["MEKIT", "IMP"] as $database)
    {
      while ($localItem = $this->getNextLocalItem($database))
      {
        $this->counters["cache"]["index"]++;
        $this->saveLocalItemInCache($localItem);
      }
    }
  }

  protected function updateRemoteFromCache()
  {
    $this->log("updating remote...");
    $this->cacheDb->resetItemWalker();
    $this->counters["remote"]["index"] = 0;

    $tmpWhere = '';
    //$tmpWhere = 'WHERE imp_metodo_client_code_c = "C  5420"';

    while ($cacheItem = $this->cacheDb->getNextItem('metodo_last_update_time_c', 'DESC', $tmpWhere))
    {
      $this->counters["remote"]["index"]++;
      $remoteItem = $this->saveRemoteItem($cacheItem);

      //@todo: re-enable!!!
      $this->storeCrmIdForCachedItem($cacheItem, $remoteItem);
    }

  }


  /**
   * @param \stdClass $cacheItem
   * @param \stdClass $remoteItem
   */
  protected function storeCrmIdForCachedItem($cacheItem, $remoteItem)
  {
    if ($remoteItem)
    {
      $cacheUpdateItem = new \stdClass();
      $cacheUpdateItem->id = $cacheItem->id;
      if (isset($remoteItem->updateFailure) && $remoteItem->updateFailure)
      {
        $this->log("CACHE-STORAGE(failure): resetting last update time");
        //we must remove crm_id and reset crm_last_update_time_c on $cacheItem
        $cacheUpdateItem->crm_id = NULL;
        $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
        $cacheUpdateItem->crm_last_update_time_c = $oldDate->format("c");
      }
      else
      {
        $remoteItemIdList = $remoteItem->ids;
        $remoteItemId = $remoteItemIdList[0];
        $this->log("CACHE-STORAGE(pass): updating timestamp for crmid: " . $remoteItemId);
        $cacheUpdateItem->crm_id = $remoteItemId;
        $now = new \DateTime();
        $cacheUpdateItem->crm_last_update_time_c = $now->format("c");
      }
      $this->cacheDb->updateItem($cacheUpdateItem);
    }
  }

  /**
   * @param \stdClass $cacheItem
   * @return \stdClass|bool
   */
  protected function saveRemoteItem($cacheItem)
  {
    $result = FALSE;
    $ISO = 'Y-m-d\TH:i:sO';
    //$metodoLastUpdate = new \DateTime();
    $metodoLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->metodo_last_update_time_c);
    $crmLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->crm_last_update_time_c);

    if ($metodoLastUpdate > $crmLastUpdate)
    {
      $this->log(
        "-----------------------------------------------------------------------------------------"
        . $this->counters["remote"]["index"]
      );

      try
      {
        $crm_id = $this->loadRemoteItemId($cacheItem);
      } catch(\Exception $e)
      {
        $this->log($e->getMessage());
        $this->log("CANNOT LOAD ID FROM CRM - UPDATE WILL BE SKIPPED");
        return $result;
      }

      $syncItem = clone($cacheItem);
      unset($syncItem->crm_id);
      unset($syncItem->id);

      //add payload to syncItem

      $payload = $this->getLocalItemPayload($cacheItem);

      $currencyFields = [
        'fatturato_storico_c',
        'ft_periodo_attuale_c',
        'ft_periodo_mobile_c',
        'ft_anno_meno_uno_c',
        'mesimobili12_c',
        'ft_anno_meno_uno_completo_c',
        'mesi12mobiliannom1_c',
        'dagenaoggi_c',
        'stessoanno1_c',
        'inizioannostesso1_c',
        'mkt_ft_periodo_attuale_c',
        'mkt_ft_periodo_mobile_c',
        'mkt_ft_anno_meno_uno_c',
        'mkt_mesimobili12_c',
        'mkt_ft_anno_meno_uno_complet_c',
        'mkt_mesi12mobiliannom1_c'
      ];

      if ($payload)
      {
        foreach ($payload as $key => $payloadData)
        {

          $payloadData = ConversionHelper::cleanupFromUnknownChars($payloadData);
          $syncItem->$key = $payloadData;


          //CODICE AGENTE (NO SPACES)
          if (in_array($key, ['imp_agent_code_c', 'mekit_agent_code_c']))
          {
            $syncItem->$key = $this->fixMetodoCode($payloadData, ['A'], TRUE);
          }

          //SETTORE IMP MEKIT  - CUSTOM FIELD NAMES
          //in $payload strtolower($database) . "_settore,
          if (in_array($key, ['imp_settore', 'mekit_settore']))
          {
            $customKey = ($key == 'imp_settore' ? 'industry' : 'mekit_industry_c');
            $syncItem->$customKey = $payloadData;
            unset($syncItem->$key);
          }

          //DATI FATTURATO - CHANGE PREFIX
          if (preg_match('#^(imp|mekit)_fatturato_#', $key, $m))
          {
            $dbPrefix = $m[1];
            $customKey = FALSE;
            if ($dbPrefix == 'imp')
            {//fields are called without db - so removing imp_
              $customKey = str_replace('imp_', '', $key);
            }
            else if ($dbPrefix == 'mekit')
            {//fields are called mkt_... (not mekit_...)
              $customKey = str_replace('mekit_', 'mkt_', $key);
            }
            if ($customKey)
            {
              $syncItem->$customKey = $payloadData;
              unset($syncItem->$key);
              $key = $customKey;
            }
          }

          // FIX Currency fields
          if (preg_match('#^fatturato_(thisyear|lastyear)_[0-9]{1,2}_c#', $key) || in_array($key, $currencyFields))
          {
            $syncItem->$key = ConversionHelper::fixCurrency($payloadData);
          }

          //FIX PERCENTS
          if (preg_match('#ft_perc_att_(mob|amu)_c#', $key)
              || preg_match('#inizioannostesso1_c#', $key)
              || preg_match('#mesi12mobiliannom1_c#', $key)
          )
          {
            //$this->log($key);
            $syncItem->$key = ConversionHelper::fixNumber($payloadData, 2);
          }
        }

        //
        //$this->log("SYNC ITEM: " . print_r($syncItem, true));


        //rename VAT NUMBER
        $syncItem->vat_number_c = $syncItem->partita_iva_c;
        unset($syncItem->partita_iva_c);

        //reformat date
        $syncItem->metodo_last_update_time_c = $metodoLastUpdate->format("Y-m-d H:i:s");

        //additional data
        $syncItem->to_be_profiled_c = FALSE;//"Da profilare"


        $restOperation = "INSERT";
        if ($crm_id)
        {
          $syncItem->id = $crm_id;
          $restOperation = "UPDATE";
        }

        //create arguments for rest
        $arguments = [
          'module_name' => 'Accounts',
          'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($syncItem),
        ];

        $this->log("CRM SYNC ITEM[$restOperation][$crm_id]");
        //$this->log(print_r($arguments, true));

        try
        {
          $result = $this->sugarCrmRest->comunicate('set_entries', $arguments);

          $this->log("REMOTE RESULT: " . json_encode($result));
          if ($result == NULL)
          {
            $this->log("ARGUMENTS: " . print_r($arguments, TRUE));
          }

        } catch(\Exception $e)
        {
          //go ahead with false silently
          $this->log("REMOTE ERROR!!! - " . $e->getMessage());
          //we must remove crm_id from $cacheItem
          //create fake result
          $result = new \stdClass();
          $result->updateFailure = TRUE;
        }
      }
    }

    return $result;
  }

  /**
   * Remote(CRM) items cannot be identified by ID because if we reset cache table(removing remote crm_id reference)
   * They would be recreated all over again
   * @param \stdClass $cacheItem
   * @return string|bool
   * @throws \Exception
   */
  protected function loadRemoteItemId($cacheItem)
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

    //identify by codice metodo - the first one found
    $fieldNames = $this->getNonEmptyMetodoCodeFieldNamesFromCacheItem($cacheItem);
    if (!count($fieldNames))
    {
      //This should never happen!!!
      throw new \Exception("CacheItem does not have usable code to get Crm ID!");
    }
    $codeFieldName = $fieldNames[0];
    $codeFieldValue = $cacheItem->$codeFieldName;
    $arguments['query'] = "accounts_cstm." . $codeFieldName . " = '" . $codeFieldValue . "'";

    /** @var \stdClass $result */
    $result = $this->sugarCrmRest->comunicate('get_entry_list', $arguments);
    //$this->log("RES: " . json_encode($result));

    if (isset($result) && isset($result->entry_list))
    {
      if (count($result->entry_list) > 1)
      {
        //This should never happen!!!
        $this->log(str_repeat("-", 120));
        $this->log(str_repeat("-", 120));
        $this->log(str_repeat("-", 120));
        $this->log(
          "There is a multiple correspondence for requested codes!"
          . json_encode($arguments), Logger::ERROR, $result->entry_list
        );
        $this->log("RESULTS: " . json_encode($result->entry_list));
        $this->log(str_repeat("-", 120));
        $this->log(str_repeat("-", 120));
        $this->log(str_repeat("-", 120));
        throw new \Exception(
          "There is a multiple correspondence for requested codes!" . json_encode($arguments)
        );
      }
      if (count($result->entry_list) == 1)
      {
        /** @var \stdClass $remoteItem */
        $remoteItem = $result->entry_list[0];
        //$this->log("FOUND REMOTE ITEM: " . json_encode($remoteItem));
        $crm_id = $remoteItem->id;
      }
    }
    else
    {
      throw new \Exception("No server response for Crm ID query!");
    }

    $this->log("CRMID (${codeFieldName} = '${codeFieldValue}' ) - " . ($crm_id ? "FOUND" : "NOT FOUND"));

    return ($crm_id);
  }

  /**
   * @param \stdClass $localItem
   * @throws \Exception
   */
  protected function saveLocalItemInCache($localItem)
  {
    /** @var string $operation */
    $operation = FALSE;
    /** @var \stdClass $cachedItem */
    $cachedItem = FALSE;
    /** @var \stdClass $cacheUpdateItem */
    $cacheUpdateItem = FALSE;
    /** @var string $identifiedBy */
    $identifiedBy = FALSE;
    /** @var string $remoteFieldNameForCodiceMetodo */
    $remoteFieldNameForCodiceMetodo = $this->getRemoteFieldNameForCodiceMetodo($localItem->database, $localItem->Tipologia);
    /** @var string $remoteFieldNameForClienteDiFatturazione */
    $remoteFieldNameForClienteDiFatturazione = $this->getRemoteFieldNameForClienteDiFatturazione($localItem->database, $localItem->Tipologia);

    //if there are more than one candidates we need to choose the one where CM === CdF
    $inversedType = ($localItem->Tipologia == "C" ? "F" : "C");
    $inversedFieldNameForCM = $this->getRemoteFieldNameForCodiceMetodo($localItem->database, $inversedType);
    $inversedFieldNameForCdF = $this->getRemoteFieldNameForClienteDiFatturazione($localItem->database, $inversedType);

    /** @var array $warnings */
    $warnings = [];

    //control by: PartitaIva
    if (!empty($localItem->PartitaIva) && $localItem->PartitaIva != '00000000000')
    {
      $filter = [
        'partita_iva_c' => $localItem->PartitaIva,
      ];
      $candidates = $this->cacheDb->loadItems($filter);
      if ($candidates)
      {
        //find by already registered Code
        foreach ($candidates as $candidate)
        {
          if ($localItem->CodiceMetodo == $candidate->$remoteFieldNameForCodiceMetodo)
          {
            $cachedItem = $candidate;
            $operation = "update";
            $identifiedBy = "PIVA + CM";
            break;
          }
        }

        if (!$operation)
        {
          if ($localItem->CodiceMetodo == $localItem->ClienteDiFatturazione)
          {
            foreach ($candidates as $candidate)
            {
              //if ($candidate->$remoteFieldNameForCodiceMetodo == $candidate->$remoteFieldNameForClienteDiFatturazione) {
              if ($candidate->$inversedFieldNameForCM == $candidate->$inversedFieldNameForCdF)
              {
                $cachedItem = $candidate;
                $operation = "update";
                $identifiedBy = "PIVA + (CM===CF)";
                break;
              }
            }
          }
        }

        if (!$operation)
        {
          //$operation = "insert";
          if (count($candidates) == 1)
          {
            $cachedItem = $candidates[0];
            $operation = "update";
            $identifiedBy = "PIVA";
          }
        }
      }
    }

    //control by: CodiceFiscale
    if (!$operation && !empty($localItem->CodiceFiscale))
    {
      $filter = [
        'codice_fiscale_c' => $localItem->CodiceFiscale,
      ];
      $candidates = $this->cacheDb->loadItems($filter);
      if ($candidates)
      {
        foreach ($candidates as $candidate)
        {
          if ($localItem->CodiceMetodo == $candidate->$remoteFieldNameForCodiceMetodo)
          {
            $cachedItem = $candidate;
            $operation = "update";
            $identifiedBy = "CODFISC + CM";
            break;
          }
        }

        if (!$operation)
        {
          if ($localItem->CodiceMetodo == $localItem->ClienteDiFatturazione)
          {
            foreach ($candidates as $candidate)
            {
              if ($candidate->$inversedFieldNameForCM == $candidate->$inversedFieldNameForCdF)
              {
                $cachedItem = $candidate;
                $operation = "update";
                $identifiedBy = "CODFISC + (CM===CF)";
                break;
              }
            }
          }
        }

        if (!$operation)
        {
          //$operation = "insert";
          $identifiedBy = "CODFISC";
        }
      }
    }

    //control by: Codice Metodo
    if (!$operation)
    {
      $filter = [
        $remoteFieldNameForCodiceMetodo => $localItem->CodiceMetodo
      ];
      $candidates = $this->cacheDb->loadItems($filter);
      if (count($candidates) > 1)
      {
        throw new \Exception(
          "Duplicati per codice metodo(" . $localItem->CodiceMetodo . ") in field: " . $remoteFieldNameForCodiceMetodo
        );
      }
      if ($candidates)
      {
        $cachedItem = $candidates[0];
        $operation = "update";
        $identifiedBy = "CM";
      }
      else
      {
        $operation = "insert";
        $identifiedBy = "NOPE";
      }
    }

    //create item for: update
    if ($operation == "update")
    {
      $cacheUpdateItem = clone($cachedItem);

      //CHECK FOR BAD DUPLICATES IN METODO
      /*
       * Se identifichiamo un $cachedItem per partitaIva o Codice Fiscale ma il codice metodo attuale($localItem->CodiceMetodo)
       * è diverso dal codice metodo che si trova sul $cachedItem, vuol dire che in Metodo abbiamo più di un anagrafica
       * registrata con la stessa PI/CF
       * Quindi in questo caso invalidiamo il $cachedItem e creiamo nuovo
       */
      if ((!empty($cachedItem->$remoteFieldNameForCodiceMetodo)
           && $localItem->CodiceMetodo != $cachedItem->$remoteFieldNameForCodiceMetodo)
          || (!empty($cachedItem->$remoteFieldNameForClienteDiFatturazione)
              && $localItem->ClienteDiFatturazione != $cachedItem->$remoteFieldNameForClienteDiFatturazione)
      )
      {
        $warnings[] = "-----------------------------------------------------------------------------------------";
        $warnings[] = "RESOLVING CONFLICT[" . $localItem->database . "]($remoteFieldNameForCodiceMetodo): "
                      . $localItem->CodiceMetodo . " -> " . $cachedItem->$remoteFieldNameForCodiceMetodo;
        $warnings[] = $localItem->CodiceMetodo . " = " . $localItem->RagioneSociale;
        $warnings[] = $cachedItem->$remoteFieldNameForCodiceMetodo . " = " . $cachedItem->name;
        $cachedItem = FALSE;
        $operation = "insert";
        $identifiedBy = "CONFLICT";
      }
      else
      {
        $metodoLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->DataDiModifica);
        $cachedDataDiModifica = new \DateTime($cachedItem->metodo_last_update_time_c);
        if ($metodoLastUpdateTime > $cachedDataDiModifica)
        {
          $cacheUpdateItem->metodo_last_update_time_c = $metodoLastUpdateTime->format("c");
        }
      }
    }

    //create item for: insert
    if ($operation == "insert")
    {
      $cacheUpdateItem = new \stdClass();
      $cacheUpdateItem->id = md5($localItem->CodiceMetodo . "-" . microtime(TRUE));

      $metodoLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->DataDiModifica);
      $cacheUpdateItem->metodo_last_update_time_c = $metodoLastUpdateTime->format("c");

      $crmLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
      $cacheUpdateItem->crm_last_update_time_c = $crmLastUpdateTime->format("c");
    }


    //add shared data on item (ONLY CODES)
    $cacheUpdateItem->$remoteFieldNameForCodiceMetodo = $localItem->CodiceMetodo;
    $cacheUpdateItem->$remoteFieldNameForClienteDiFatturazione = $localItem->ClienteDiFatturazione;

    if (!empty($localItem->PartitaIva) && $localItem->PartitaIva != "00000000000")
    {
      $cacheUpdateItem->partita_iva_c = $localItem->PartitaIva;
    }
    if (!empty($localItem->CodiceFiscale))
    {
      $cacheUpdateItem->codice_fiscale_c = $localItem->CodiceFiscale;
    }

    //codice agente - moved under getLocalItemPayload
    //        if(!empty($localItem->CodiceAgente)) {
    //            if($localItem->database == "IMP") {
    //                $cacheUpdateItem->imp_agent_code_c = $localItem->CodiceAgente;
    //            } else if($localItem->database == "MEKIT") {
    //                $cacheUpdateItem->mekit_agent_code_c = $localItem->CodiceAgente;
    //            }
    //        }

    //DECIDE OPERATION(better to keep this off for now)
    $operation = ($cachedItem == $cacheUpdateItem) ? "skip" : $operation;

    //add other data on item

    //@todo: this field can be deleted from crm: crm_export_flag_c
    $cacheUpdateItem->crm_export_flag_c = $localItem->CrmExportFlag;
    $cacheUpdateItem->name = $localItem->RagioneSociale;

    if ($operation != "skip")
    {
      $this->log(
        "-----------------------------------------------------------------------------------------"
        . $this->counters["cache"]["index"]
      );
      $this->log(
        "[" . $localItem->database . "][$operation][$identifiedBy]-" . "[" . $localItem->CodiceMetodo . "]" . "["
        . $localItem->ClienteDiFatturazione . "]" . " " . $localItem->RagioneSociale . ""
      );
      $this->log("CACHED: " . json_encode($cachedItem));
      $this->log("UPDATE: " . json_encode($cacheUpdateItem));
    }
    if (!empty($warnings))
    {
      foreach ($warnings as $warning)
      {
        $this->log("WARNING: " . $warning);
      }
    }

    switch ($operation)
    {
      case "insert":
        $this->cacheDb->addItem($cacheUpdateItem);
        break;
      case "update":
        $this->cacheDb->updateItem($cacheUpdateItem);
        break;
      case "skip":
        break;
      default:
        throw new \Exception("Operation($operation) is not implemented!");
    }
  }

  /**
   * @param \stdClass $cacheItem
   * @return array|bool
   */
  protected function getLocalItemPayload($cacheItem)
  {
    $answer = FALSE;

    try
    {
      $headData = $this->getLocalItemPayloadHeadData($cacheItem);
      //$this->log("HEAD DATA: " . json_encode($headData, JSON_PRETTY_PRINT));


      //$invoiceData = FALSE;
      $invoiceData = $this->getLocalItemPayloadInvoiceData($cacheItem);
      //$this->log("INVOICE DATA: " . json_encode($invoiceData, JSON_PRETTY_PRINT));

      if (is_array($headData))
      {
        $answer = $headData;
        if (is_array($invoiceData))
        {
          $answer = array_merge($answer, $invoiceData);
        }
      }

    } catch(\Exception $e)
    {
      $this->log("ERROR GETTING ITEM PAYLOAD: " . $e->getMessage());
    }

    //$this->log("PAYLOAD DATA: " . json_encode($answer, JSON_PRETTY_PRINT));
    return $answer;
  }

  /**
   * @param \stdClass $cacheItem
   * @return array|bool
   * @throws \Exception
   */
  protected function getLocalItemPayloadInvoiceData($cacheItem)
  {
    $answer = FALSE;
    $db = Configuration::getDatabaseConnection("SERVER2K8");
    $databases = ["IMP", "MEKIT"];
    $fieldNames = $this->getNonEmptyMetodoCodeFieldNamesFromCacheItem($cacheItem);
    $items = [];
    foreach ($databases as $database)
    {
      $metodoCodes = [];
      foreach ($fieldNames as $fieldName)
      {
        if (preg_match("#^" . strtolower($database) . "_metodo_client_code_c$#", $fieldName))
        {
          $metodoCodes[] = "'" . $cacheItem->$fieldName . "'";
        }
      }
      if (count($metodoCodes))
      {
        $sql = "SELECT
                    FTDATA.CodiceMetodo,
                    FTDATA.F0 AS " . strtolower($database) . "_fatturato_storico_c,
                    FTDATA.F1Anno AS " . strtolower($database) . "_fatturato_thisyear_1_c,
                    FTDATA.F2Anno AS " . strtolower($database) . "_fatturato_thisyear_2_c,
                    FTDATA.F3Anno AS " . strtolower($database) . "_fatturato_thisyear_3_c,
                    FTDATA.F4Anno AS " . strtolower($database) . "_fatturato_thisyear_4_c,
                    FTDATA.F5Anno AS " . strtolower($database) . "_fatturato_thisyear_5_c,
                    FTDATA.F6Anno AS " . strtolower($database) . "_fatturato_thisyear_6_c,
                    FTDATA.F7Anno AS " . strtolower($database) . "_fatturato_thisyear_7_c,
                    FTDATA.F8Anno AS " . strtolower($database) . "_fatturato_thisyear_8_c,
                    FTDATA.F9Anno AS " . strtolower($database) . "_fatturato_thisyear_9_c,
                    FTDATA.F10Anno AS " . strtolower($database) . "_fatturato_thisyear_10_c,
                    FTDATA.F11Anno AS " . strtolower($database) . "_fatturato_thisyear_11_c,
                    FTDATA.F12Anno AS " . strtolower($database) . "_fatturato_thisyear_12_c,
                    
                    FTDATA.F1AnnoPrec AS " . strtolower($database) . "_fatturato_lastyear_1_c,
                    FTDATA.F2AnnoPrec AS " . strtolower($database) . "_fatturato_lastyear_2_c,
                    FTDATA.F3AnnoPrec AS " . strtolower($database) . "_fatturato_lastyear_3_c,
                    FTDATA.F4AnnoPrec AS " . strtolower($database) . "_fatturato_lastyear_4_c,
                    FTDATA.F5AnnoPrec AS " . strtolower($database) . "_fatturato_lastyear_5_c,
                    FTDATA.F6AnnoPrec AS " . strtolower($database) . "_fatturato_lastyear_6_c,
                    FTDATA.F7AnnoPrec AS " . strtolower($database) . "_fatturato_lastyear_7_c,
                    FTDATA.F8AnnoPrec AS " . strtolower($database) . "_fatturato_lastyear_8_c,
                    FTDATA.F9AnnoPrec AS " . strtolower($database) . "_fatturato_lastyear_9_c,
                    FTDATA.F10AnnoPrec AS " . strtolower($database) . "_fatturato_lastyear_10_c,
                    FTDATA.F11AnnoPrec AS " . strtolower($database) . "_fatturato_lastyear_11_c,
                    FTDATA.F12AnnoPrec AS " . strtolower($database) . "_fatturato_lastyear_12_c
                    
                    FROM [$database].[dbo].[SogCRM_AnagraficaCF_2016_12_31] AS FTDATA                    
                    WHERE FTDATA.CodiceMetodo IN (" . implode(",", $metodoCodes) . ")
                    ";

        //@todo: TEMPORARY - FOR JANUARY 2017 - MUST FIX INVOICE DATA DECEMBER - CUSTOM LOGIC VITO
        // original table: FROM [$database].[dbo].[SogCRM_AnagraficaCF] AS FTDATA

        //@todo: TEMPORARY - FOR JANUARY 2017 - MUST FIX INVOICE DATA DECEMBER - CUSTOM LOGIC VITO

        $statement = $db->prepare($sql);
        $statement->execute();
        $itemList = $statement->fetchAll(\PDO::FETCH_ASSOC);
        if (count($itemList))
        {
          $items = array_merge($items, $itemList);
        }
      }
    }

    if (count($items))
    {
      $answer = [];

      //sanitize items
      foreach ($items as &$item)
      {
        foreach ($item as $itemKey => &$itemData)
        {
          $itemData = floatval(trim($itemData));
        }
      }

      //merge to single item
      foreach ($items as &$item)
      {
        $answer = array_merge($answer, $item);
      }

      $answer = $this->doInvoiceDataAnalysis($answer);
    }

    return $answer;
  }


  /**
   * @param \stdClass $cacheItem
   * @return array|bool
   * @throws \Exception
   */
  protected function getLocalItemPayloadHeadData($cacheItem)
  {
    $answer = FALSE;
    $db = Configuration::getDatabaseConnection("SERVER2K8");
    $databases = ["IMP", "MEKIT"];
    $fieldNames = $this->getNonEmptyMetodoCodeFieldNamesFromCacheItem($cacheItem);
    $items = [];
    foreach ($databases as $database)
    {
      $metodoCodes = [];
      foreach ($fieldNames as $fieldName)
      {
        if (preg_match("#^" . strtolower($database) . "_metodo_(client|supplier)_code_c$#", $fieldName))
        {
          $metodoCodes[] = "'" . $cacheItem->$fieldName . "'";
        }
      }
      if (count($metodoCodes))
      {
        $sql = "SELECT
                    ACF.DATAMODIFICA AS last_updated_at,
                    ACF.INDIRIZZO AS billing_address_street,
                    ACF.CAP AS billing_address_postalcode,
                    ACF.LOCALITA AS billing_address_city,
                    ACF.PROVINCIA AS billing_address_state,
                    ACF.CODICEISO AS billing_address_country,
                    ACF.TELEFONO AS phone_office,
                    ACF.FAX AS phone_fax,
                    ACF.TELEX AS email1,
                    ACF.INDIRIZZOINTERNET AS website,
                    ACF.NOTE AS " . strtolower($database) . "_metodo_notes_c,
                    ACFR.CODAGENTE1 AS " . strtolower($database) . "_agent_code_c,
                    ACFR.CODZONA AS zone_c,
                    ACFR.CODSETTORE AS " . strtolower($database) . "_settore
                    FROM [$database].[dbo].[ANAGRAFICACF] AS ACF
                    INNER JOIN [$database].[dbo].[ANAGRAFICARISERVATICF] AS ACFR ON ACF.CODCONTO = ACFR.CODCONTO
                    AND ACFR.ESERCIZIO = (SELECT TOP (1) TE.CODICE FROM [$database].[dbo].[TABESERCIZI] AS TE ORDER BY TE.CODICE DESC)
                    WHERE ACF.CODCONTO IN (" . implode(",", $metodoCodes) . ")
                    ";

        $statement = $db->prepare($sql);
        $statement->execute();
        $itemList = $statement->fetchAll(\PDO::FETCH_ASSOC);
        if (count($itemList))
        {
          $items = array_merge($items, $itemList);
        }
      }
    }

    if (count($items))
    {
      $answer = [];

      //sanitize items
      foreach ($items as &$item)
      {
        foreach ($item as $itemKey => &$itemData)
        {
          if (!empty(trim($itemData)))
          {
            $itemData = trim($itemData);
            //special cases
            if ($itemKey == "billing_address_country")
            {
              $itemData = ($itemData == "IT" ? "ITALIA" : $itemData);
            }
          }
          else
          {
            unset($item[$itemKey]);
          }
        }
        $item["last_updated_at"] = \DateTime::createFromFormat('Y-m-d H:i:s.u', $item["last_updated_at"]);
        ksort($item);
      }

      //Sort by last_updated_at date ascending so that more recent is last element
      usort(
        $items, function ($item1, $item2)
      {
        if ($item1['last_updated_at'] == $item2['last_updated_at'])
        {
          return 0;
        }
        return ($item1['last_updated_at'] > $item2['last_updated_at']) ? 1 : -1;
      }
      );

      //merge to single item
      foreach ($items as &$item)
      {
        unset($item["last_updated_at"]);//no need for this anymore
        $answer = array_merge($answer, $item);
      }
    }

    return $answer;
  }

  /**
   * @param array $payload
   * @return array
   */
  protected function doInvoiceDataAnalysis($payload)
  {
    //$this->log("INVOICE CALC PAYLOAD " . json_encode($payload));

    $databases = ["IMP", "MEKIT"];
    foreach ($databases as $database)
    {
      //$this->log(str_repeat("-", 80) . $database);
      /*
       * 3 mesi - Periodo attuale
       * -------------------------
       * totale 3 mesi precedenti al mese corrente(47,908.08)
       */
      $key = $database == "IMP" ? "ft_periodo_attuale_c" : "mkt_ft_periodo_attuale_c";
      //$fields = $this->getInvoiceDataFieldNamesBackwards($database, 3);
      $fields = $this->getInvoiceDataFieldNamesBackwards($database, 3, -1);//forced for 2016-12-31
      $value_periodo_attuale_3 = $this->getInvoiceDataFieldsSum($payload, $fields);
      //$this->log("PERIODO ATTUALE 3 MESI($key): " . json_encode($fields) . " = " . $value_periodo_attuale_3);
      $payload[$key] = $value_periodo_attuale_3;

      /*
       * 3 mesi - Periodo mobile
       * -------------------------
       * totale 3 mesi precedenti al periodo attuale
       */
      $key = $database == "IMP" ? "ft_periodo_mobile_c" : "mkt_ft_periodo_mobile_c";
      //$fields = $this->getInvoiceDataFieldNamesBackwards($database, 3, 3);
      $fields = $this->getInvoiceDataFieldNamesBackwards($database, 3, 2);//forced for 2016-12-31
      $value_periodo_mobile_3 = $this->getInvoiceDataFieldsSum($payload, $fields);
      //$this->log("PERIODO MOBILE 3 MESI($key): " . json_encode($fields) . " = " . $value_periodo_mobile_3);
      $payload[$key] = $value_periodo_mobile_3;

      /*
       * 3 mesi - Periodo anno - 1
       * -------------------------
       * totale 3 mesi precedenti al mese corrente dell'anno scorso
       */
      $key = $database == "IMP" ? "ft_anno_meno_uno_c" : "mkt_ft_anno_meno_uno_c";
      //$fields = $this->getInvoiceDataFieldNamesBackwards($database, 3, 12);
      $fields = $this->getInvoiceDataFieldNamesBackwards($database, 3, 11);//forced for 2016-12-31
      $value_periodo_anno_meno_uno_3 = $this->getInvoiceDataFieldsSum($payload, $fields);
      //$this->log("PERIODO ANNO MENO UNO 3 MESI($key): " . json_encode($fields) . " = " .
      // $value_periodo_anno_meno_uno_3);
      $payload[$key] = $value_periodo_anno_meno_uno_3;

      /*
       * 3 mesi - Periodo Attuale / Periodo Mobile (%)
       * -------------------------
       * rapporto tra periodo attuale e periodo mobile
       */
      $key = $database == "IMP" ? "ft_perc_att_mob_c" : "mkt_ft_perc_att_mob_c";
      $value_perc_att_mob = 0;
      if ($value_periodo_mobile_3 != 0)
      {
        $value_perc_att_mob = 100 * ($value_periodo_attuale_3 - $value_periodo_mobile_3) / $value_periodo_mobile_3;
      }
      //$this->log("PERCENTUALE ATTUALE SU MOBILE 3 MESI($key): " . " = " . $value_perc_att_mob);
      $payload[$key] = $value_perc_att_mob;

      /*
       * 3 mesi - Periodo Attuale / Periodo anno - 1 (%)
       * -------------------------
       * rapporto tra periodo attuale e periodo anno - 1
       */
      $key = $database == "IMP" ? "ft_perc_att_amu_c" : "mkt_ft_perc_att_amu_c";
      $value_perc_att_amu = 0;
      if ($value_periodo_anno_meno_uno_3 != 0)
      {
        $value_perc_att_amu = 100 * ($value_periodo_attuale_3 - $value_periodo_anno_meno_uno_3)
                              / $value_periodo_anno_meno_uno_3;
      }
      //$this->log("PERCENTUALE ATTUALE SU ANNO-1 3 MESI($key): " . " = " . $value_perc_att_amu);
      $payload[$key] = $value_perc_att_amu;


      /*
       * 12 mesi - Periodo mobile
       * -------------------------
       * totale 12 mesi precedenti al mese corrente
       */
      $key = $database == "IMP" ? "mesimobili12_c" : "mkt_mesimobili12_c";
      //$fields = $this->getInvoiceDataFieldNamesBackwards($database, 12);
      $fields = $this->getInvoiceDataFieldNamesBackwards($database, 12, -1);//forced for 2016-12-31
      $value_periodo_mobile_12 = $this->getInvoiceDataFieldsSum($payload, $fields);
      //$this->log("PERIODO MOBILE 12 MESI($key): " . json_encode($fields) . " = " . $value_periodo_mobile_12);
      $payload[$key] = $value_periodo_mobile_12;

      /*
       * 12 mesi - Anno scorso
       * -------------------------
       * totale 12 mesi dell'anno scorso
       */
      $key = $database == "IMP" ? "ft_anno_meno_uno_completo_c" : "mkt_ft_anno_meno_uno_complet_c";
      $fields = $this->getInvoiceDataFieldNamesLastYear($database);
      $value_periodo_anno_meno_uno_12 = $this->getInvoiceDataFieldsSum($payload, $fields);
      //$this->log("PERIODO ANNO MENO UNO 12 MESI($key): " . json_encode($fields) . " = " .
      //$value_periodo_anno_meno_uno_12);
      $payload[$key] = $value_periodo_anno_meno_uno_12;

      /*
       * 12 mesi - Periodo Mobile / Periodo Anno Scorso (%)
       * -------------------------
       * rapporto tra periodo mobile e periodo anno-1
       */
      $key = $database == "IMP" ? "mesi12mobiliannom1_c" : "mkt_mesi12mobiliannom1_c";
      $value_perc_mob_lastyr = 0;
      if ($value_periodo_anno_meno_uno_12 != 0)
      {
        $value_perc_mob_lastyr = 100 * ($value_periodo_mobile_12 - $value_periodo_anno_meno_uno_12)
                                 / $value_periodo_anno_meno_uno_12;
      }
      //$this->log("PERCENTUALE MOBILE SU ANNO SCORSO 12 MESI($key): " . " = " . $value_perc_mob_lastyr);
      $payload[$key] = $value_perc_mob_lastyr;


      /*
       * ANNO IN CORSO - Da Gen. a oggi
       * -------------------------
       * totale mesi precedenti al mese corrente fino all'inizio dell'anno
       */
      $key = $database == "IMP" ? "dagenaoggi_c" : "mkt_dagenaoggi_c";
      $fields = $this->getInvoiceDataFieldNamesFromBeginningOfYear($database, 0);
      $value_anno_in_corso_mesi = $this->getInvoiceDataFieldsSum($payload, $fields);
      //$this->log("PERIODO ANNO IN CORSO($key): " . json_encode($fields) . " = " . $value_anno_in_corso_mesi);
      $payload[$key] = $value_anno_in_corso_mesi;

      /*
       * ANNO SCORSO - Da Gen. a oggi
       * -------------------------
       * totale mesi precedenti al mese corrente fino all'inizio dell'anno scorso
       */
      $key = $database == "IMP" ? "stessoanno1_c" : "mkt_stessoanno1_c";
      $fields = $this->getInvoiceDataFieldNamesFromBeginningOfYear($database, 12);
      $value_anno_scorso_mesi = $this->getInvoiceDataFieldsSum($payload, $fields);
      //$this->log("PERIODO ANNO SCORSO($key): " . json_encode($fields) . " = " . $value_anno_scorso_mesi);
      $payload[$key] = $value_anno_scorso_mesi;

      /*
       * Rapporto - Anno in corso / Anno Scorso (%)
       * -------------------------
       * rapporto tra anno in corso e annno scorso (stesso periodo)
       */
      $key = $database == "IMP" ? "inizioannostesso1_c" : "mkt_inizioannostesso1_c";
      $value_perc_curryr_lastyr = 0;
      if ($value_anno_scorso_mesi != 0)
      {
        $value_perc_curryr_lastyr = 100 * ($value_anno_in_corso_mesi - $value_anno_scorso_mesi)
                                    / $value_anno_scorso_mesi;
      }
      //$this->log("PERCENTUALE ANNO IN CORSO / ANNO SCORSO($key): " . " = " . $value_perc_curryr_lastyr);
      $payload[$key] = $value_perc_curryr_lastyr;
    }
    return $payload;
  }

  /**
   * @param array $payload
   * @param array $fields
   * @return float
   */
  protected function getInvoiceDataFieldsSum($payload, $fields)
  {
    $answer = 0;
    foreach ($fields as $fieldName)
    {
      if (isset($payload[$fieldName]))
      {
        $answer += floatval($payload[$fieldName]);
      }
    }
    return $answer;
  }

  /**
   * Returns field names for the specified length starting from (and excluding) current month
   *
   * INVOICE CALC PAYLOAD {
   * "imp_fatturato_storico_c":"2598.6000",
   *
   * "imp_fatturato_thisyear_1_c":"0.0000",
   * "imp_fatturato_thisyear_2_c":"0.0000",
   * "imp_fatturato_thisyear_3_c":"0.0000",
   * "imp_fatturato_thisyear_4_c":"0.0000",
   * "imp_fatturato_thisyear_5_c":"0.0000",
   * "imp_fatturato_thisyear_6_c":"0.0000",
   * "imp_fatturato_thisyear_7_c":"0.0000",
   * "imp_fatturato_thisyear_8_c":"0.0000",
   * "imp_fatturato_thisyear_9_c":"0.0000",
   * "imp_fatturato_thisyear_10_c":"0.0000",
   * "imp_fatturato_thisyear_11_c":"0.0000",
   * "imp_fatturato_thisyear_12_c":"0.0000",
   *
   * "imp_fatturato_lastyear_1_c":"0.0000",
   * "imp_fatturato_lastyear_2_c":"0.0000",
   * "imp_fatturato_lastyear_3_c":"0.0000",
   * "imp_fatturato_lastyear_4_c":"0.0000",
   * "imp_fatturato_lastyear_5_c":"0.0000",
   * "imp_fatturato_lastyear_6_c":"0.0000",
   * "imp_fatturato_lastyear_7_c":"0.0000",
   * "imp_fatturato_lastyear_8_c":"0.0000",
   * "imp_fatturato_lastyear_9_c":"0.0000",
   * "imp_fatturato_lastyear_10_c":"0.0000",
   * "imp_fatturato_lastyear_11_c":"0.0000",
   * "imp_fatturato_lastyear_12_c":"0.0000"
   * }
   *
   * @param string $database
   * @param int    $length
   * @param int    $offset
   * @return array
   */
  protected function getInvoiceDataFieldNamesBackwards($database, $length, $offset = 0)
  {
    $answer = [];
    //$now = new \DateTime();
    $now = \DateTime::createFromFormat('Y-m-d', '2016-12-31');
    $currMonthNumber = (int) $now->format("n");
    $dbPrefix = strtolower($database) == "imp" ? "imp" : "mkt";
    $fieldPrefix = $dbPrefix . "_fatturato_";
    for ($m = 1; $m <= $length; $m++)
    {
      $currentYearIndicator = "thisyear";
      $monthNumber = $currMonthNumber - $offset - $m;
      if ($monthNumber < 1)
      {
        $monthNumber += 12;
        $currentYearIndicator = "lastyear";
      }
      $answer[] = $fieldPrefix . $currentYearIndicator . "_" . $monthNumber . "_c";
    }
    return $answer;
  }

  /**
   * @param string $database
   * @return array
   */
  protected function getInvoiceDataFieldNamesLastYear($database)
  {
    $answer = [];
    $dbPrefix = strtolower($database) == "imp" ? "imp" : "mkt";
    $fieldPrefix = $dbPrefix . "_fatturato_";
    for ($monthNumber = 1; $monthNumber <= 12; $monthNumber++)
    {
      $currentYearIndicator = "lastyear";
      $answer[] = $fieldPrefix . $currentYearIndicator . "_" . $monthNumber . "_c";
    }
    return $answer;
  }

  /**
   * @param string $database
   * @param int    $offset
   * @return array
   */
  protected function getInvoiceDataFieldNamesFromBeginningOfYear($database, $offset = 0)
  {
    $answer = [];
    //$now = new \DateTime();
    $now = \DateTime::createFromFormat('Y-m-d', '2016-12-31');
    //$currMonthNumber = (int) $now->format("n");
    $currMonthNumber = (int) $now->format("n") + 1;//forced for 2016-12-31
    $dbPrefix = strtolower($database) == "imp" ? "imp" : "mkt";
    $fieldPrefix = $dbPrefix . "_fatturato_";
    for ($m = 1; $m < $currMonthNumber; $m++)
    {
      $currentYearIndicator = "thisyear";
      $monthNumber = $m - $offset;
      if ($monthNumber < 1)
      {
        $monthNumber += 12;
        $currentYearIndicator = "lastyear";
      }
      $answer[] = $fieldPrefix . $currentYearIndicator . "_" . $monthNumber . "_c";
    }
    return $answer;
  }


  /**
   * @param string $database IMP|MEKIT
   * @return bool|\stdClass
   */
  protected function getNextLocalItem($database)
  {
    if (!$this->localItemStatement)
    {
      $db = Configuration::getDatabaseConnection("SERVER2K8");
      $sql = "SELECT
                ACF.CODCONTO AS CodiceMetodo,
                ACF.TIPOCONTO AS Tipologia,
                ACF.CODFISCALE AS CodiceFiscale,
                ACF.PARTITAIVA AS PartitaIva,
                ACF.DSCCONTO1 AS Nome1,
                ACF.DSCCONTO2 AS Nome2,
                ACF.DATAMODIFICA AS DataDiModifica,
                ACFR.CODCONTOFATT AS ClienteDiFatturazione,
                CrmExportFlag = CASE
                     WHEN EXTC.SOGCRM_Esportabile IS NOT NULL THEN EXTC.SOGCRM_Esportabile
                     WHEN EXTF.SOGCRM_Esportabile IS NOT NULL THEN EXTF.SOGCRM_Esportabile
                     ELSE 1 END
                FROM [$database].[dbo].[ANAGRAFICACF] AS ACF
                INNER JOIN [$database].[dbo].[ANAGRAFICARISERVATICF] AS ACFR ON ACF.CODCONTO = ACFR.CODCONTO AND ACFR.ESERCIZIO = (SELECT TOP (1) TE.CODICE FROM [$database].[dbo].[TABESERCIZI] AS TE ORDER BY TE.CODICE DESC)
                LEFT JOIN [$database].dbo.EXTRACLIENTI AS EXTC ON ACF.CODCONTO = EXTC.CODCONTO
                LEFT JOIN [$database].dbo.EXTRAFORNITORI AS EXTF ON ACF.CODCONTO = EXTF.CODCONTO

                ORDER BY ACF.CODCONTO ASC
                ";//ACF.DATAMODIFICA

      /*
       * RIMOSSO TEMPORANEAMENTE ESPORTABILE
       *                 WHERE EXTC.SOGCRM_Esportabile <> 0
                OR EXTF.SOGCRM_Esportabile <> 0
       * */

      $this->localItemStatement = $db->prepare($sql);
      $this->localItemStatement->execute();
    }

    $item = $this->localItemStatement->fetch(\PDO::FETCH_OBJ);

    if ($item)
    {
      $item->CodiceMetodo = trim($item->CodiceMetodo);
      $item->database = $database;

      //the field 'DSCCONTO2' is set to 'ANAGRAFICA INCOMPLETA' when inserting new CF in Metodo
      //This should NOT be part of the name of the Account
      $item->RagioneSociale = $item->Nome1;
      if (!empty($item->Nome2) && $item->Nome2 != 'ANAGRAFICA INCOMPLETA')
      {
        $item->RagioneSociale .= ' - ' . $item->Nome2;
      }

    }
    else
    {
      $this->localItemStatement = NULL;
    }
    return $item;
  }

  /**
   * @param \stdClass $cacheItem
   * @return array
   * @throws \Exception
   */
  protected function getNonEmptyMetodoCodeFieldNamesFromCacheItem($cacheItem)
  {
    $answer = [];
    $fields = [
      "imp_metodo_client_code_c",     //imp_metodo_client_code_c
      "imp_metodo_supplier_code_c",   //imp_metodo_supplier_code_c
      "mekit_metodo_client_code_c",   //mekit_metodo_client_code_c
      "mekit_metodo_supplier_code_c"  //mekit_metodo_supplier_code_c
    ];
    foreach ($fields as $fieldName)
    {
      if (isset($cacheItem->$fieldName) && !empty($cacheItem->$fieldName))
      {
        $answer[] = $fieldName;
      }
    }
    if (!count($answer))
    {
      throw new \Exception("No non-empty field names can be found on cache item!");
    }
    return $answer;
  }

  /**
   * @param string $database
   * @param string $type
   * @return string
   * @throws \Exception
   */
  protected function getRemoteFieldNameForCodiceMetodo($database, $type)
  {
    switch ($database)
    {
      case "IMP":
        switch ($type)
        {
          case "C":
            $answer = "imp_metodo_client_code_c";       //imp_metodo_client_code_c
            break;
          case "F":
            $answer = "imp_metodo_supplier_code_c";     //imp_metodo_supplier_code_c
            break;
          default:
            throw new \Exception("Local item needs to have Tipologia C|F!");
        }
        break;
      case "MEKIT":
        switch ($type)
        {
          case "C":
            $answer = "mekit_metodo_client_code_c";     //mekit_metodo_client_code_c
            break;
          case "F":
            $answer = "mekit_metodo_supplier_code_c";   //mekit_metodo_supplier_code_c
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

  /**
   * @param string $database
   * @param string $type
   * @return string
   * @throws \Exception
   */
  protected function getRemoteFieldNameForClienteDiFatturazione($database, $type)
  {
    switch ($database)
    {
      case "IMP":
        switch ($type)
        {
          case "C":
            $answer = "imp_metodo_invoice_client_c";       //imp_metodo_invoice_client_c
            break;
          case "F":
            $answer = "imp_metodo_invoice_supplier_c";       //imp_metodo_invoice_supplier_c
            break;
          default:
            throw new \Exception("Local item needs to have Tipologia C|F!");
        }
        break;
      case "MEKIT":
        switch ($type)
        {
          case "C":
            $answer = "mekit_metodo_invoice_client_c";     //mekit_metodo_invoice_client_c
            break;
          case "F":
            $answer = "mekit_metodo_invoice_supplier_c";     //mekit_metodo_invoice_supplier_c
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

  /**
   * @param string $originalCode
   * @param array  $prefixes
   * @param bool   $nospace - Do NOT space prefix from number - new crm cannot have spaces in dropdowns
   * @return string
   */
  protected function fixMetodoCode($originalCode, $prefixes, $nospace = FALSE)
  {
    $normalizedCode = '';
    if (!empty($originalCode))
    {
      $codeLength = 7;
      $normalizedCode = '';
      $PREFIX = strtoupper(substr($originalCode, 0, 1));
      $NUMBER = trim(substr($originalCode, 1));
      $SPACES = '';
      if (in_array($PREFIX, $prefixes))
      {
        if (0 != (int) $NUMBER)
        {
          if (!$nospace)
          {
            $SPACES = str_repeat(' ', $codeLength - strlen($PREFIX) - strlen($NUMBER));
          }
          $normalizedCode = $PREFIX . $SPACES . $NUMBER;
        }
        else
        {
          //$this->log("UNSETTING BAD CODE[not numeric]: '" . $originalCode . "'");
        }
      }
      else
      {
        //$this->log("UNSETTING BAD CODE[not C|F]: '" . $originalCode . "'");
      }
    }
    return $normalizedCode;
  }

}