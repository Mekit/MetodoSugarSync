<?php
/**
 * Created by Adam Jakab.
 * Date: 25/07/17
 * Time: 10.13
 */

namespace Mekit\Sync\MetodoToCrm;

use Mekit\Console\Configuration;
use Mekit\DbCache\AccountCache;
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

  /** @var string */
  protected $clientCode;

  /** @var  \stdClass */
  protected $clientData;

  /** @var  AccountCache */
  protected $accountCacheDb;

  /**
   * @param callable $logger
   */
  public function __construct($logger)
  {
    parent::__construct($logger);
    $this->accountCacheDb = new AccountCache('Account', $logger);
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
      $clientCode = $arguments["client-code"];
      $this->updateWithClientCode($clientCode);
    }
    else
    {
      $this->updateWithClientCodesFromCache();
    }

  }

  /**
   * Execute for all IMP codes present in the cache
   */
  protected function updateWithClientCodesFromCache()
  {
    $hasMore = TRUE;
    $codes = [];
    $where = 'WHERE imp_metodo_client_code_c IS NOT NULL';

    while ($hasMore)
    {
      try
      {
        $cacheItem = $this->accountCacheDb->getNextItem('imp_metodo_client_code_c', 'ASC', $where);
        if ($cacheItem && isset($cacheItem->imp_metodo_client_code_c))
        {
          $codes[] = $cacheItem->imp_metodo_client_code_c;
        }
        else
        {
          $hasMore = FALSE;
        }
      } catch(\Exception $e)
      {
        //no problem - we'll do it next time
      }
    }

    $this->log("Found number of client codes: " . count($codes));

    while ($clientCode = array_pop($codes))
    {
      $this->updateWithClientCode($clientCode);
    }
  }

  /**
   * @param string $clientCode
   * @return bool
   */
  protected function updateWithClientCode($clientCode)
  {
    try
    {
      $this->clientCode = ConversionHelper::checkClientCode($clientCode);

      $this->log("Updating for client code: '" . $this->clientCode . "'");

      $this->clientData = new \stdClass();
      $this->getGenericData();
      $this->getCurrentMonthData();
      $this->getDeadlinesData();
      $this->getRecentlyBoughtArticlesData();
      $this->getRecentlyNotBoughtArticlesData();
      $this->markNotBoughtArticles();

      //$this->log("CLIENT DATA: \n" . print_r($this->clientData, TRUE));
      $res = $this->saveRemoteItem();
    } catch(\Exception $e)
    {
      $this->log("ERROR(updateWithClientCode): " . $e->getMessage());
      $res = FALSE;
    }

    return $res;
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

    $syncItem->client_data = base64_encode(serialize($this->clientData->generic));
    $syncItem->current_month = base64_encode(serialize($this->clientData->current_month));
    $syncItem->deadlines = base64_encode(serialize($this->clientData->deadlines));
    $syncItem->products_recent_buys = base64_encode(serialize($this->clientData->recently_bought_articles));
    $syncItem->products_recent_non_buys = base64_encode(serialize($this->clientData->recently_not_bought_articles));

    //create arguments for rest
    $arguments = [
      'module_name' => 'mkt_AccountExtras',
      'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($syncItem),
    ];

    //$this->log("CRM SYNC ITEM[$restOperation][$extra_id]");
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
   * We need do mark articles present in recently_bought_articles
   * if they are also present in recently_not_bought_articles
   */
  protected function markNotBoughtArticles()
  {
    foreach ($this->clientData->recently_not_bought_articles as $nbArticle)
    {
      $nbCode = $nbArticle["CodArt"];
      foreach ($this->clientData->recently_bought_articles as &$bArticle)
      {
        $bCode = $bArticle["CodArt"];
        $isInNotBoughtList = isset($bArticle["isInNotBoughtList"]) ? $bArticle["isInNotBoughtList"] : 0;
        $isInNotBoughtList = ($nbCode == $bCode) ? 1 : $isInNotBoughtList;
        $bArticle["isInNotBoughtList"] = $isInNotBoughtList;
      }
    }
  }

  /**
   * (ARTICOLI NON ACQUISTATI RECENTEMENTE)
   */
  protected function getRecentlyNotBoughtArticlesData()
  {
    $db = Configuration::getDatabaseConnection("SERVER2K8");
    $sql = "SELECT
              D.*
              FROM IMP.dbo.Sog_SpecchiettoOrdiniClientiArticoliNonAcquistatiMesiRecenti AS D
              WHERE D.CodCliFor = '" . $this->clientCode . "'
              ORDER BY D.DataUltimoAcq DESC
              ";

    $statement = $db->prepare($sql);
    $statement->execute();
    $rows = $statement->fetchAll();

    if (count($rows))
    {
      foreach ($rows as &$row)
      {
        //unsetting numeric keys on arrays
        for ($i = 0; $i < 25; $i++)
        {
          unset($row[$i]);
        }

        //fix
        $d = new \DateTime($row["DataUltimoAcq"]);
        $row["DataUltimoAcq"] = $d->format("Y-m-d");
        //
        $row["TotQtaGest"] = ConversionHelper::fixNumber($row["TotQtaGest"], 2, '.');
        $row["PrezzoUnitNettoEuroUltimoAcq"] = ConversionHelper::fixNumber($row["PrezzoUnitNettoEuroUltimoAcq"], 2, '.');
        $row["PrezzoListino42"] = ConversionHelper::fixNumber($row["PrezzoListino42"], 2, '.');

        //remove useless
        unset($row["CodCliFor"]);
        unset($row["DesCliFor"]);
        unset($row["CodCliForFatt"]);
        unset($row["DesCliForFatt"]);
      }
    }

    $this->clientData->recently_not_bought_articles = $rows;
  }

  /**
   * (ARTICOLI ACQUISTATI RECENTEMENTE)
   */
  protected function getRecentlyBoughtArticlesData()
  {
    $db = Configuration::getDatabaseConnection("SERVER2K8");
    $sql = "SELECT
              D.*
              FROM IMP.dbo.Sog_SpecchiettoOrdiniClientiUltimiMesi AS D
              WHERE D.CodCliFor = '" . $this->clientCode . "'
              ORDER BY TotRigaListino42 DESC
              ";

    $statement = $db->prepare($sql);
    $statement->execute();
    $rows = $statement->fetchAll();

    if (count($rows))
    {
      foreach ($rows as &$row)
      {
        //unsetting numeric keys on arrays
        for ($i = 0; $i < 25; $i++)
        {
          unset($row[$i]);
        }

        //fix
        $d = new \DateTime($row["DataDoc"]);
        $row["DataDoc"] = $d->format("Y-m-d");

        $row["QtaGest"] = ConversionHelper::fixNumber($row["QtaGest"], 2, '.');
        $row["QtaGestRes"] = ConversionHelper::fixNumber($row["QtaGestRes"], 2, '.');
        $row["PrezzoUnitNettoEuro"] = ConversionHelper::fixNumber($row["PrezzoUnitNettoEuro"], 2, '.');
        $row["PrezzoListino42"] = ConversionHelper::fixNumber($row["PrezzoListino42"], 2, '.');
        $row["TotNettoRigaEuro"] = ConversionHelper::fixNumber($row["TotNettoRigaEuro"], 2, '.');
        $row["TotNettoRigaEuroRes"] = ConversionHelper::fixNumber($row["TotNettoRigaEuroRes"], 2, '.');
        $row["TotRigaListino42"] = ConversionHelper::fixNumber($row["TotRigaListino42"], 2, '.');
        $row["TotRigaListino42Res"] = ConversionHelper::fixNumber($row["TotRigaListino42Res"], 2, '.');

        //remove useless
        unset($row["CodCliFor"]);
        unset($row["DesCliFor"]);
        unset($row["CodCliForFatt"]);
        unset($row["DesCliForFatt"]);
      }
    }

    $this->clientData->recently_bought_articles = $rows;
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
              ORDER BY DataScadenza DESC
              ";

    $statement = $db->prepare($sql);
    $statement->execute();
    $rows = $statement->fetchAll();

    if (count($rows))
    {
      foreach ($rows as &$row)
      {
        //unsetting numeric keys on arrays
        for ($i = 0; $i < 25; $i++)
        {
          unset($row[$i]);
        }

        //fix
        $d = new \DateTime($row["DataScadenza"]);
        $row["DataScadenza"] = $d->format("Y-m-d");

        $d = new \DateTime($row["DataFattura"]);
        $row["DataFattura"] = $d->format("Y-m-d");

        $row["ImportoScEuro"] = ConversionHelper::fixNumber($row["ImportoScEuro"], 2, '.');

        //remove useless
        unset($row["CodCliForFatt"]);
        unset($row["DesCliForFatt"]);
      }
    }

    $this->clientData->deadlines = $rows;
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
              ORDER BY TotNettoRigaEuro DESC
              ";

    $statement = $db->prepare($sql);
    $statement->execute();
    $rows = $statement->fetchAll();

    if (count($rows))
    {

      //unsetting numeric keys on arrays
      foreach ($rows as &$row)
      {

        for ($i = 0; $i < 25; $i++)
        {
          unset($row[$i]);
        }

        //fix
        $d = new \DateTime($row["DataDoc"]);
        $row["DataDoc"] = $d->format("Y-m-d");

        $row["QtaGest"] = ConversionHelper::fixNumber($row["QtaGest"], 0);
        $row["QtaGestRes"] = ConversionHelper::fixNumber($row["QtaGestRes"], 0);

        $row["PrezzoUnitNettoEuro"] = ConversionHelper::fixNumber($row["PrezzoUnitNettoEuro"], 2, '.');
        $row["PrezzoListino42"] = ConversionHelper::fixNumber($row["PrezzoListino42"], 2, '.');
        $row["TotNettoRigaEuro"] = ConversionHelper::fixNumber($row["TotNettoRigaEuro"], 2, '.');
        $row["TotNettoRigaEuroRes"] = ConversionHelper::fixNumber($row["TotNettoRigaEuroRes"], 2, '.');
        $row["TotRigaListino42"] = ConversionHelper::fixNumber($row["TotRigaListino42"], 2, '.');
        $row["TotRigaListino42Res"] = ConversionHelper::fixNumber($row["TotRigaListino42Res"], 2, '.');
      }
    }

    $this->clientData->current_month = $rows;
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
              ACFR.CODCONTOFATT AS ClienteDiFatturazione,
              ACFR.CODAGENTE1 AS Agente1
              FROM [$this->dbName].[dbo].[ANAGRAFICACF] AS ACF
              INNER JOIN [$this->dbName].[dbo].[ANAGRAFICARISERVATICF] AS ACFR ON ACF.CODCONTO = ACFR.CODCONTO AND ACFR.ESERCIZIO = (SELECT TOP (1) TE.CODICE FROM [$this->dbName].[dbo].[TABESERCIZI] AS TE ORDER BY TE.CODICE DESC)
              WHERE ACF.CODCONTO = '" . $this->clientCode . "'
              ";

    $statement = $db->prepare($sql);
    $statement->execute();
    $item = $statement->fetch(\PDO::FETCH_OBJ);

    if ($item)
    {
      //fix
      $d = new \DateTime($item->DataDiModifica);
      $item->DataDiModifica = $d->format("Y-m-d H:i:s");

      $item->CodiceMetodo = trim($item->CodiceMetodo);
      $item->Database = $this->dbName;

      $this->clientData->generic = $item;
    }
  }
}