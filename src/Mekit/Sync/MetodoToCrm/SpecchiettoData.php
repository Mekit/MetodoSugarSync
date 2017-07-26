<?php
/**
 * Created by Adam Jakab.
 * Date: 25/07/17
 * Time: 10.13
 */

namespace Mekit\Sync\MetodoToCrm;

use Mekit\Console\Configuration;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRest;
use Mekit\Sync\ConversionHelper;
use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;

class SpecchiettoData extends Sync implements SyncInterface
{

  /** @var callable */
  protected $logger;

  /** @var SugarCrmRest */
  protected $sugarCrmRest;

  /** @var string */
  protected $dbName = 'IMP';

  /** @var array */
  protected $clientCode = 'C  3981';//MEKIT('C  3981')

  /** @var  \stdClass */
  protected $clientData;

  /**
   * @param callable $logger
   */
  public function __construct($logger)
  {
    parent::__construct($logger);
    $this->sugarCrmRest = new SugarCrmRest();
  }

  /**
   * @param array $options
   * @param array $arguments
   */
  public function execute($options, $arguments)
  {
    //$this->log("EXECUTING..." . json_encode($options));
    //$this->log("EXECUTING..." . json_encode($arguments));

    if (isset($arguments["client-code"]) && $arguments["client-code"])
    {
      $this->clientCode = ConversionHelper::checkClientCode($arguments["client-code"]);
    }

    $this->log("EXECUTING for client code: " . $this->clientCode);

    $this->clientData = new \stdClass();

    $this->getGenericData();
    $this->getCurrentMonthData();
    $this->getDeadlinesData();

    $this->log("CLIENT DATA: \n" . print_r($this->clientData, TRUE));

    $res = $this->saveRemoteItem();
  }


  /**
   * @return bool
   */
  protected function saveRemoteItem()
  {
    $result = FALSE;
    $account_id = NULL;
    $extra_id = NULL;

    try
    {
      $ids = $this->loadRemoteAccountExtraIds();
      if (!$ids)
      {
        throw new \Exception("Unable to get Account id!");
      }

      $account_id = $ids["account_id"];
      $extra_id = $ids["extra_id"];

    } catch(\Exception $e)
    {
      $this->log($e->getMessage());
      $this->log("--- UPDATE WILL BE SKIPPED ---");
      return $result;
    }

    //$this->log("Account ID: " . $account_id);
    //$this->log("Extra ID: " . $extra_id);

    $syncItem = new \stdClass();

    $restOperation = "INSERT";
    if ($extra_id)
    {
      $syncItem->id = $extra_id;
      $restOperation = "UPDATE";
    }

    $syncItem->name = $this->clientData->generic->Nome1;
    $syncItem->description = '';

    $syncItem->dati_cliente = implode(
      "\n", [
      "codice metodo: " . $this->clientData->generic->CodiceMetodo,
      "cliente di fatturazione: " . $this->clientData->generic->ClienteDiFatturazione,
      "codice fiscale: " . $this->clientData->generic->CodiceFiscale,
      "partita iva: " . $this->clientData->generic->PartitaIva,
    ]
    );


    $syncItem->current_month = $this->clientData->current_month_txt;
    $syncItem->deadlines = $this->clientData->deadlines_txt;


    //create arguments for rest
    $arguments = [
      'module_name' => 'mkt_AccountExtras',
      'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($syncItem),
    ];

    $this->log("CRM SYNC ITEM[$restOperation][$extra_id]");
    //$this->log("ARGS:\n" . print_r($arguments, true));

    try
    {
      $result = $this->sugarCrmRest->comunicate('set_entries', $arguments);
      //$this->log("REMOTE RESULT: " . print_r($result, true));
    } catch(\Exception $e)
    {
      $this->log("REMOTE ERROR!!! - " . $e->getMessage());
    }

    //create relationship to account only if this was an INSERT
    if ($restOperation == "INSERT")
    {
      if (isset($result->ids[0]) && !empty($result->ids[0]))
      {
        $extra_id = $result->ids[0];

        $arguments = [
          'module_name' => 'mkt_AccountExtras',
          'module_id' => $extra_id,
          'link_field_name' => 'mkt_accountextras_accounts',
          'related_ids' => [$account_id]
        ];

        $result = $this->sugarCrmRest->comunicate('set_relationship', $arguments);
        if (!isset($result->created) || $result->created != 1)
        {
          $this->log("RELATIONSHIP ERROR!!! - " . json_encode($arguments));
        }
        else
        {
          //$this->log("RELATIONSHIP RESULT: " . json_encode($result));
        }
      }
    }

    return $result;
  }

  /**
   * Loads Account and related AccountExtra ID
   * and returns
   * [account_id, extra_id]
   *
   * @throws \Exception
   * @return bool|array
   */
  protected function loadRemoteAccountExtraIds()
  {
    $answer = FALSE;
    $account_id = FALSE;
    $extra_id = FALSE;

    $arguments = [
      'module_name' => 'Accounts',
      'query' => "",
      'order_by' => "",
      'offset' => 0,
      'select_fields' => ['id'],
      'link_name_to_fields_array' => [
        [
          'name' => 'mkt_accountextras_accounts',
          'value' => ['id'],
        ]
      ],
      'max_results' => 2,
      'deleted' => FALSE,
      'Favorites' => FALSE,
    ];

    $codeFieldName = "imp_metodo_client_code_c";
    $arguments['query'] = "accounts_cstm." . $codeFieldName . " = '" . $this->clientCode . "'";

    /** @var \stdClass $result */
    $result = $this->sugarCrmRest->comunicate('get_entry_list', $arguments);
    //$this->log("RES-1: " . print_r($result, TRUE));

    if ($result === FALSE || (isset($result->result_count) && $result->result_count > 1))
    {
      throw new \Exception("Cannot determin unique CRM ID!");
    }
    else
    {
      if ($result->result_count == 1)
      {
        // Account ID
        if (isset($result->entry_list) && count($result->entry_list) == 1)
        {
          /** @var \stdClass $remoteItem */
          $remoteItem = $result->entry_list[0];
          $account_id = $remoteItem->id;
        }

        // Extra ID
        if (isset($result->relationship_list) && count($result->relationship_list) == 1)
        {
          /** @var \stdClass $relationship_list_element */
          $relationship_list_element = $result->relationship_list[0];
          if (isset($relationship_list_element->link_list) && count($relationship_list_element->link_list) == 1)
          {
            /** @var \stdClass $link_list_element */
            $link_list_element = $relationship_list_element->link_list[0];
            if (isset($link_list_element->records) && count($link_list_element->records) == 1)
            {
              /** @var \stdClass $record_element */
              $record_element = $link_list_element->records[0];
              if (isset($record_element->link_value->id->value))
              {
                if (!empty($record_element->link_value->id->value))
                {
                  $extra_id = $record_element->link_value->id->value;
                }
              }
            }
          }
        }
      }
    }

    //account is necessary to be there
    if ($account_id)
    {
      $answer = [
        'account_id' => $account_id,
        'extra_id' => $extra_id
      ];
    }

    return $answer;
  }

  /**
   * Deadlines
   */
  protected function getDeadlinesData()
  {
    $db = Configuration::getDatabaseConnection("SERVER2K8");
    $sql = "SELECT
              D.*
              FROM IMP.dbo.Sog_SpecchiettoOrdiniClientiScadenzeAperte AS D
              WHERE D.CodCliForFatt = '" . $this->clientCode . "'
              ";

    $statement = $db->prepare($sql);
    $statement->execute();
    $this->clientData->deadlines = $statement->fetchAll();

    if (count($this->clientData->deadlines))
    {
      $schiantation = '';

      foreach ($this->clientData->deadlines as &$item)
      {
        //unsetting numeric keys on arrays
        for ($i = 0; $i < 25; $i++)
        {
          unset($item[$i]);
        }

        //fix
        $d = new \DateTime($item["DataScadenza"]);
        $item["DataScadenza"] = $d->format("Y-m-d");

        $d = new \DateTime($item["DataFattura"]);
        $item["DataFattura"] = $d->format("Y-m-d");

        $item["ImportoScEuro"] = ConversionHelper::fixNumber($item["ImportoScEuro"], 2);

        //remove useless
        unset($item["CodCliForFatt"]);
        unset($item["DesCliForFatt"]);

        $schiantation .= implode(" | ", $item) . "\n";
      }

      //column headers
      $itemZero = $this->clientData->deadlines[0];
      $this->clientData->deadlines_txt .= implode(" | ", array_keys($itemZero)) . "\n";
      $this->clientData->deadlines_txt .= $schiantation;

    }

  }

  /**
   * Current Month
   */
  protected function getCurrentMonthData()
  {
    $db = Configuration::getDatabaseConnection("SERVER2K8");
    $sql = "SELECT
              D.DataDoc,
              D.CodArt,
              D.DesArt,
              D.QtaGest,
              D.QtaGestRes,
              D.PrezzoUnitNettoEuro,
              D.PrezzoListino42,
              D.TotNettoRigaEuro,
              D.TotNettoRigaEuroRes,
              D.TotRigaListino42,
              D.TotRigaListino42Res
              FROM IMP.dbo.Sog_SpecchiettoOrdiniClientiMeseInCorso AS D
              WHERE D.CodCliFor = '" . $this->clientCode . "'
              ";

    $statement = $db->prepare($sql);
    $statement->execute();
    $this->clientData->current_month = $statement->fetchAll();
    $this->clientData->current_month_txt = "";

    if (count($this->clientData->current_month))
    {
      $schiantation = '';

      foreach ($this->clientData->current_month as &$item)
      {
        //unsetting numeric keys on arrays
        for ($i = 0; $i < 25; $i++)
        {
          unset($item[$i]);
        }

        //fix
        $d = new \DateTime($item["DataDoc"]);
        $item["DataDoc"] = $d->format("Y-m-d");

        $item["QtaGest"] = ConversionHelper::fixNumber($item["QtaGest"], 0);
        $item["QtaGestRes"] = ConversionHelper::fixNumber($item["QtaGestRes"], 0);

        $item["PrezzoUnitNettoEuro"] = ConversionHelper::fixNumber($item["PrezzoUnitNettoEuro"], 2);
        $item["PrezzoListino42"] = ConversionHelper::fixNumber($item["PrezzoListino42"], 2);
        $item["TotNettoRigaEuro"] = ConversionHelper::fixNumber($item["TotNettoRigaEuro"], 2);
        $item["TotNettoRigaEuroRes"] = ConversionHelper::fixNumber($item["TotNettoRigaEuroRes"], 2);
        $item["TotRigaListino42"] = ConversionHelper::fixNumber($item["TotRigaListino42"], 2);
        $item["TotRigaListino42Res"] = ConversionHelper::fixNumber($item["TotRigaListino42Res"], 2);


        $schiantation .= implode(" | ", $item) . "\n";
      }

      //column headers
      $itemZero = $this->clientData->current_month[0];
      $this->clientData->current_month_txt .= implode(" | ", array_keys($itemZero)) . "\n";
      $this->clientData->current_month_txt .= $schiantation;
    }

  }

  /**
   * Dati anagrafici
   */
  protected function getGenericData()
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
              ACFR.CODCONTOFATT AS ClienteDiFatturazione
              FROM [$this->dbName].[dbo].[ANAGRAFICACF] AS ACF
              INNER JOIN [$this->dbName].[dbo].[ANAGRAFICARISERVATICF] AS ACFR ON ACF.CODCONTO = ACFR.CODCONTO AND ACFR.ESERCIZIO = (SELECT TOP (1) TE.CODICE FROM [$this->dbName].[dbo].[TABESERCIZI] AS TE ORDER BY TE.CODICE DESC)
              WHERE ACF.CODCONTO = '" . $this->clientCode . "'
              ";

    $statement = $db->prepare($sql);
    $statement->execute();
    $item = $statement->fetch(\PDO::FETCH_OBJ);

    if ($item)
    {
      $item->CodiceMetodo = trim($item->CodiceMetodo);
      $item->database = $this->dbName;

      $this->clientData->generic = $item;
    }
  }
}