<?php
/**
 * Created by Adam Jakab.
 * Date: 22/03/16
 * Time: 16.08
 */

namespace Mekit\Sync\CsvToCrm;

use League\Csv\Reader;
use Mekit\Console\Configuration;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRest;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRestException;
use Mekit\Sync\ConversionHelper;
use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;

class GenericAccounts extends Sync implements SyncInterface
{
  /** @var callable */
  protected $logger;

  /** @var SugarCrmRest */
  protected $sugarCrmRest;

  /** @var int */
  protected $counter;

  /** AbstractCsv */
  protected $CSV;

  /** @var  array */
  protected $csvHeaders;


  /**
   * @param callable $logger
   */
  public function __construct($logger)
  {
    parent::__construct($logger);
    $this->sugarCrmRest = new SugarCrmRest();

    $cfg = Configuration::getConfiguration();
    $this->CSV = Reader::createFromPath($cfg['global']['datafile']);
    $this->csvHeaders = $this->CSV->fetchOne();
  }

  /**
   * @param array $options
   */
  public function execute($options)
  {
    $this->log("EXECUTING..." . json_encode($options));

    $lineNumber = 1;
    $FORCE_LIMIT = 1;

    while ($csvItem = $this->getCsvLine($lineNumber))
    {
      if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counter >= $FORCE_LIMIT)
      {
        $this->log("Forced limit($FORCE_LIMIT) reached.");
        break;
      }

      $csvItem = $this->fixCsvData($csvItem);
      $this->log("LINE($lineNumber): " . json_encode($csvItem));


      $this->updateAccount($csvItem, $options["update-remote"]);

      $lineNumber++;
      $this->counter++;
    }
  }

  /**
   * @param array $csvItem
   * @return mixed
   */
  protected function fixCsvData($csvItem)
  {
    foreach (array_keys($csvItem) as $k)
    {
      $csvItem[$k] = trim($csvItem[$k]);
    }

    $csvItem['agente_imp'] = $this->getAgentCodeFor($csvItem["agente_imp"]);
    $csvItem['agente_ex_imp'] = $this->getAgentCodeFor($csvItem["agente_ex_imp"]);

    $date = \DateTime::createFromFormat('d/m/Y', $csvItem["date_start_rapp"]);
    $formattedDate = $date ? $date->format('Y-m-d') : '';
    $csvItem['date_start_rapp'] = $formattedDate;

    $date = \DateTime::createFromFormat('d/m/Y', $csvItem["date_last_sleep"]);
    $formattedDate = $date ? $date->format('Y-m-d') : '';
    $csvItem['date_last_sleep'] = $formattedDate;

    $csvItem['state_code_imp'] = $this->getStateCode($csvItem["phase_code"]);

    $csvItem['discount_2015'] = ConversionHelper::fixNumber($csvItem["discount_2015"]);
    $csvItem['discount_2016'] = ConversionHelper::fixNumber($csvItem["discount_2016"]);

    $csvItem['revenue_2015'] = ConversionHelper::fixNumber($csvItem["revenue_2015"]);
    $csvItem['revenue_2016'] = ConversionHelper::fixNumber($csvItem["revenue_2016"]);

    $csvItem['prev_fatt_10'] = ConversionHelper::fixNumber($csvItem["prev_fatt_10"]);
    $csvItem['prev_fatt_50'] = ConversionHelper::fixNumber($csvItem["prev_fatt_50"]);
    $csvItem['prev_fatt_100'] = ConversionHelper::fixNumber($csvItem["prev_fatt_100"]);
    $csvItem['prev_fatt_tot'] = ConversionHelper::fixNumber($csvItem["prev_fatt_tot"]);

    $csvItem['mean_discount'] = ConversionHelper::fixNumber($csvItem["mean_discount"]);

    $csvItem['contact_tm'] = ucfirst(strtolower($csvItem['contact_tm']));

    $csvItem['metodo_sync'] = ($csvItem['metodo_sync'] == 1);

    $csvItem['state_forced'] = ($csvItem['state_forced'] == 1);


    return $csvItem;
  }

  /**
   * @param array $csvItem
   * @param bool  $doUpdate
   * @throws \Exception
   */
  protected function updateAccount($csvItem, $doUpdate = FALSE)
  {
    //CSV -> OBJ
    $csv2ObjMap = [
      'Id' => 'id',
      'agente_imp' => 'imp_agent_code_c',
      'agente_ex_imp' => 'imp_ex_agent_code_c',
      'date_start_rapp' => 'imp_acc_start_date_c',
      'date_last_sleep' => 'imp_acc_sleep_date_c',
      'state_code_imp' => 'imp_status_c',
      'phase_code' => 'imp_status_phase__c',
      'sector' => 'industry',
      'discount_2015' => 'imp_discount_2015_c',
      'discount_2016' => 'imp_discount_2016_c',
      'revenue_2015' => 'imp_revenue_2015_c',
      'revenue_2016' => 'imp_revenue_2016_c',

      'prev_fatt_10' => 'imp_prev_di_fatt_10_c',
      'prev_fatt_50' => 'imp_prev_di_fatt_50_c',
      'prev_fatt_100' => 'imp_prev_di_fatt_100_c',
      'prev_fatt_tot' => 'imp_totale_prev_fatt_c',

      'mean_discount' => 'imp_sconto_medio_c',

      'contact_tm' => 'chiamatoperbarbara_c',

      'metodo_sync' => 'imp_sync_as_client_c',

      'state_forced' => 'imp_forced_status_c',
    ];


    $syncItem = new \stdClass();
    foreach ($csv2ObjMap as $csvKey => $objKey)
    {
      if (!array_key_exists($csvKey, $csvItem))
      {
        throw new \Exception("Invalid Csv key: " . $csvKey);
      }
      $syncItem->$objKey = $csvItem[$csvKey];
    }

    //create arguments for rest
    $arguments = [
      'module_name' => 'Accounts',
      'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($syncItem),
    ];

    $this->log("\nCRM SYNC ITEM: " . json_encode($arguments));

    if ($doUpdate)
    {
      try
      {
        $result = $this->sugarCrmRest->comunicate('set_entries', $arguments);
        $this->log("REMOTE RESULT: " . json_encode($result));
      } catch(SugarCrmRestException $e)
      {
        //go ahead with false silently
        $this->log("REMOTE ERROR!!! - " . $e->getMessage());
      }
    }
  }


  /**
   * @param string $phaseCode like: 2_1
   * @return string
   * @throws \Exception
   */
  protected function getStateCode($phaseCode)
  {
    $parts = explode("_", $phaseCode);
    if (count($parts) != 2)
    {
      throw new \Exception("Invalid Phase code: " . $phaseCode);
    }

    return $parts[0];
  }

  /**
   * @param string $val
   * @return string
   * @throws \Exception
   */
  protected function getAgentCodeFor($val)
  {
    $agents = [
      '' => '',
      'Chiara Aragno(A4)' => 'A4',
      'Vito Scilabra(A5)' => 'A5',
      'Michela Tartaglia(A11)' => 'A11',
      'Lucio Loguercio(A7)' => 'A7',
      'Claudio Crocco(A8)' => 'A8',
      'Barbara Stocchiero(A9)' => 'A9',
      'Fabrizio Zollet(A14)' => 'A14',
      'BEXB(A10)' => 'A10',
      'MEPA(A12)' => 'A12',
      'IMP(A13)' => 'A13',
      'Dario Eutizi (A15)' => 'A15',
      'Davide Cazzadore(A6)' => 'A6'
    ];
    if (!array_key_exists($val, $agents))
    {
      throw new \Exception("Invalid Agent: " . $val);
    }

    return $agents[$val];
  }


  /**
   * @param array $csvItem
   * @return string|bool
   * @throws \Exception
   */
  protected function OLD_getAgentCodeFromClienteDiFatturazione($csvItem)
  {
    $agent_code = FALSE;
    $arguments = [
      'module_name' => 'Accounts',
      'query' => "accounts_cstm.imp_metodo_client_code_c = '" . $csvItem['codice_metodo_ft'] . "'",
      'order_by' => "",
      'offset' => 0,
      'select_fields' => ['id', 'imp_agent_code_c'],
      'link_name_to_fields_array' => [],
      'max_results' => 2,
      'deleted' => FALSE,
      'Favorites' => FALSE,
    ];

    /** @var \stdClass $result */
    $result = $this->sugarCrmRest->comunicate('get_entry_list', $arguments);
    if (isset($result) && isset($result->entry_list))
    {
      if (count($result->entry_list) == 1)
      {
        /** @var \stdClass $remoteItem */
        $remoteItem = $result->entry_list[0];
        if (isset($remoteItem->name_value_list->imp_agent_code_c->value))
        {
          $agent_code = $remoteItem->name_value_list->imp_agent_code_c->value;
        }
      }
    }
    return $agent_code;
  }

  /**
   * @param int $lineNumber
   * @return array|bool
   */
  protected function getCsvLine($lineNumber)
  {
    $answer = FALSE;
    if ($csvItem = $this->CSV->fetchOne($lineNumber))
    {
      $answer = $this->indexifyCsvItem($this->csvHeaders, $csvItem);
    }
    return $answer;
  }


  /**
   * @param array $headers
   * @param array $csvItem
   * @return array
   */
  protected function indexifyCsvItem($headers, $csvItem)
  {
    $answer = [];
    while (count($headers))
    {
      $h = array_pop($headers);
      $v = array_pop($csvItem);
      $answer[$h] = $v;
    }
    return $answer;
  }

}