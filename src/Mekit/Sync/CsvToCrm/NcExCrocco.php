<?php
/**
 * Created by Adam Jakab.
 * Date: 22/03/16
 * Time: 16.08
 */

namespace Mekit\Sync\CsvToCrm;


use League\Csv\Reader;
use League\Csv\AbstractCsv;
use Mekit\Console\Configuration;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRest;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRestException;
use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;
use Monolog\Logger;

class NcExCrocco extends Sync implements SyncInterface
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
   * @param array $arguments
   */
  public function execute($options, $arguments)
  {
    $this->log("EXECUTING..." . json_encode($options));


    $lineNumber = 1;
    $FORCE_LIMIT = 999;
    while ($csvItem = $this->getCsvLine($lineNumber))
    {
      if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counter >= $FORCE_LIMIT)
      {
        break;
      }
      if ($csvItem["codice_metodo_ft"])
      {
        $csvItem['imp_agent_code_c'] = $this->getAgentCodeFromClienteDiFatturazione($csvItem);
        $this->log("CSV ITEM($lineNumber): " . json_encode($csvItem));
        if ($csvItem['imp_agent_code_c'])
        {
          if (isset($options["update-remote"]) && $options["update-remote"])
          {
            $this->updateAccount($csvItem);
          }
        }
      }
      $lineNumber++;
      $this->counter++;
    }
  }


  /**
   * @param array $csvItem
   */
  protected function updateAccount($csvItem)
  {
    $syncItem = new \stdClass();
    $syncItem->id = $csvItem['Id'];
    $syncItem->imp_agent_code_c = $csvItem['imp_agent_code_c'];
    $syncItem->imp_sync_as_client_c = 1;

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
   * @param array $csvItem
   * @return string|bool
   * @throws \Exception
   */
  protected function getAgentCodeFromClienteDiFatturazione($csvItem)
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