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

      if (isset($options["update-remote"]) && $options["update-remote"])
      {
        $this->updateAccount($csvItem);
      }

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


    return $csvItem;
  }

  /**
   * @param array $csvItem
   * @throws \Exception
   */
  protected function updateAccount($csvItem)
  {
    //CSV -> OBJ
    $csv2ObjMap = [
      'Id' => 'id',
      'agente_imp' => 'imp_agent_code_c',
      'agente_ex_imp' => 'imp_ex_agent_code_c',
      'date_start_rapp' => 'imp_acc_start_date_c',
      'date_last_sleep' => 'imp_acc_sleep_date_c',
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


    //$syncItem->imp_sync_as_client_c = 1;

    //create arguments for rest
    $arguments = [
      'module_name' => 'Accounts',
      'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($syncItem),
    ];

    $this->log("CRM SYNC ITEM: " . json_encode($arguments));

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