<?php
/**
 * Created by Adam Jakab.
 * Date: 01/12/15
 * Time: 11.32
 */

namespace Mekit\Sync\Metodo\Down;


use Mekit\Console\Configuration;
use Mekit\DbCache\AccountCache;
use Mekit\Sync\SugarCrm\Rest\SugarCrmRest;
use Mekit\Sync\SugarCrm\Rest\SugarCrmRestException;

class AccountData {
    /** @var callable */
    protected $logger;

    /** @var SugarCrmRest */
    protected $sugarCrmRest;

    /** @var  AccountCache */
    protected $cacheDb;

    /** @var string */
    protected $dataIdentifier = 'Account';

    /** @var  \PDOStatement */
    protected $localItemStatement;

    /**
     * @param callable $logger
     */
    public function __construct($logger) {
        $this->logger = $logger;
        $this->cacheDb = new AccountCache($this->dataIdentifier, $logger);
        $this->sugarCrmRest = new SugarCrmRest();
    }

    public function execute() {
        $this->updateLocalCache();

    }

    protected function updateLocalCache() {
        $this->log("updating local cache...");
        foreach (["IMP", "MEKIT"] as $database) {
            $this->log("DB: $database");
            while ($localItem = $this->getNextLocalItem($database)) {
                $this->saveLocalItemInCache($localItem);
            }
        }
    }


    protected function saveLocalItemInCache($localItem) {
        $foundBy = "CODE";
        $remoteFieldNameForCodiceMetodo = $this->getRemoteFieldNameForCodiceMetodo($localItem->database, $localItem->Tipologia);
        $remoteFieldNameForClienteDiFatturazione = $this->getRemoteFieldNameForClienteDiFatturazione($localItem->database);
        $filter = [
            $remoteFieldNameForCodiceMetodo => $localItem->CodiceMetodo
        ];
        $cachedItem = $this->cacheDb->loadItem($filter);
        if (!$cachedItem && !empty($localItem->PartitaIva) && $localItem->PartitaIva != '00000000000') {
            $filter = [
                'partita_iva_c' => $localItem->PartitaIva
            ];
            $cachedItem = $this->cacheDb->loadItem($filter);
            $foundBy = "PIVA";
        }
        if (!$cachedItem && !empty($localItem->CodiceFiscale)) {
            $filter = [
                'codice_fiscale_c' => $localItem->CodiceFiscale
            ];
            $cachedItem = $this->cacheDb->loadItem($filter);
            $foundBy = "CF";
        }

        $operation = $cachedItem ? "update" : "insert";
        if ($operation == "insert") {
            $foundBy = "NOPE";
            $cacheUpdateItem = new \stdClass();
            $cacheUpdateItem->id = md5($localItem->CodiceMetodo . "-" . microtime(TRUE));
            $localDataDiModifica = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->DataDiModifica);
            $cacheUpdateItem->metodo_last_update_time_c = $localDataDiModifica->format("c");
        }
        else {
            $cacheUpdateItem = clone($cachedItem);
            $localDataDiModifica = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->DataDiModifica);
            $cachedDataDiModifica = new \DateTime($cachedItem->metodo_last_update_time_c);
            if ($localDataDiModifica > $cachedDataDiModifica) {
                $cacheUpdateItem->metodo_last_update_time_c = $localDataDiModifica->format("c");
            }
        }

        $cacheUpdateItem->$remoteFieldNameForCodiceMetodo = $localItem->CodiceMetodo;

        if (!empty($localItem->PartitaIva)) {
            $cacheUpdateItem->partita_iva_c = $localItem->PartitaIva;
        }
        if (!empty($localItem->CodiceFiscale)) {
            $cacheUpdateItem->codice_fiscale_c = $localItem->CodiceFiscale;
        }
        if (!empty($localItem->ClienteDiFatturazione)) {
            $cacheUpdateItem->cliente_di_fatturazione_c = $localItem->ClienteDiFatturazione;
        }


        $operation = ($cachedItem == $cacheUpdateItem) ? "skip" : $operation;


        if ($operation !== "skip" && $cachedItem->partita_iva_c == "06784280015") {
            $this->log(
                "-----------------------------------------------------------------------------------------FB: "
                . $foundBy
            );
            $this->log("caching item([op:$operation]): " . $localItem->CodiceMetodo);
            $this->log("CACHED: " . json_encode($cachedItem));
            $this->log("UPDATE: " . json_encode($cacheUpdateItem));
        }


        if ($operation == "insert") {
            $this->cacheDb->addItem($cacheUpdateItem);
        }
        else if ($operation == "update") {
            $this->cacheDb->updateItem($cacheUpdateItem);
        }
    }

    /**
     * @param string $database IMP|MEKIT
     * @return bool|\stdClass
     */
    protected function getNextLocalItem($database) {
        if (!$this->localItemStatement) {
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
                FROM [$database].[dbo].[ANAGRAFICACF] AS ACF
                INNER JOIN [$database].[dbo].[ANAGRAFICARISERVATICF] AS ACFR ON ACF.CODCONTO = ACFR.CODCONTO
                ";

            $this->localItemStatement = $db->prepare($sql);
            $this->localItemStatement->execute();
        }
        $item = $this->localItemStatement->fetch(\PDO::FETCH_OBJ);
        if ($item) {
            $item->CodiceMetodo = trim($item->CodiceMetodo);
            $item->database = $database;
        }
        else {
            $this->localItemStatement = NULL;
        }
        return $item;
    }

    /**
     * @param string $database
     * @return string
     * @throws \Exception
     */
    protected function getRemoteFieldNameForClienteDiFatturazione($database) {
        switch ($database) {
            case "IMP":
                $answer = "metodo_invoice_client_code_imp_c";
                break;
            case "MEKIT":
                $answer = "metodo_invoice_client_code_mekit_c";
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
    protected function getRemoteFieldNameForCodiceMetodo($database, $type) {
        switch ($database) {
            case "IMP":
                switch ($type) {
                    case "C":
                        $answer = "metodo_client_code_imp_c";
                        break;
                    case "F":
                        $answer = "metodo_supplier_code_imp_c";
                        break;
                    default:
                        throw new \Exception("Local item needs to have Tipologia C|F!");
                }
                break;
            case "MEKIT":
                switch ($type) {
                    case "C":
                        $answer = "metodo_client_code_mekit_c";
                        break;
                    case "F":
                        $answer = "metodo_supplier_code_mekit_c";
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
     * @param string $msg
     */
    protected function log($msg) {
        call_user_func($this->logger, $msg);
    }
}