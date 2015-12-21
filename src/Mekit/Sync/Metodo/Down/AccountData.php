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

    /** @var bool */
    protected $skipRemoteUpdate = FALSE;

    /**
     * @param callable $logger
     */
    public function __construct($logger) {
        $this->logger = $logger;
        $this->cacheDb = new AccountCache($this->dataIdentifier, $logger);
        $this->sugarCrmRest = new SugarCrmRest();
        $cfg = Configuration::getConfiguration();
        $this->skipRemoteUpdate = (isset($cfg["global"]["skip_remote_update"]) && $cfg["global"]["skip_remote_update"]);
    }

    public function execute() {
        $this->updateLocalCache();

    }

    protected function updateLocalCache() {
        $this->log("updating local cache...");
        foreach (["MEKIT", "IMP"] as $database) {
            while ($localItem = $this->getNextLocalItem($database)) {
                $this->saveLocalItemInCache($localItem);
            }
        }
        if (!$this->skipRemoteUpdate) {
            $this->log("updating remote...");
            $this->cacheDb->resetItemWalker();
            while ($cacheItem = $this->cacheDb->getNextItem()) {
                $remoteItem = $this->saveRemoteItem($cacheItem);
                $this->storeCrmIdForCachedItem($cacheItem, $remoteItem);
            }
        }
        else {
            $this->log("Remote update is disabled by configuration!");
        }
    }

    /**
     * @param \stdClass $cacheItem
     * @param \stdClass $remoteItem
     */
    protected function storeCrmIdForCachedItem($cacheItem, $remoteItem) {
        if ($remoteItem) {
            $cacheUpdateItem = new \stdClass();
            $cacheUpdateItem->id = $cacheItem->id;
            if (isset($remoteItem->updateFailure) && $remoteItem->updateFailure) {
                //we must remove crm_id and reset crm_last_update_time_c on $cacheItem
                $cacheUpdateItem->crm_id = NULL;
                $cacheUpdateItem->crm_last_update_time_c = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
            }
            else {
                $cacheUpdateItem->crm_id = $remoteItem->id;
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
    protected function saveRemoteItem($cacheItem) {
        $result = FALSE;

        $ISO = 'Y-m-d\TH:i:sO';
        //$metodoLastUpdate = new \DateTime();
        $metodoLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->metodo_last_update_time_c);
        $crmLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->crm_last_update_time_c);

        if ($metodoLastUpdate > $crmLastUpdate) {
            $this->log("-----------------------------------------------------------------------------------------");
            $syncItem = clone($cacheItem);
            $crm_id = $this->loadRemoteItemId($cacheItem);

            //UPDATE
            if ($crm_id) {
                $this->log("updating remote...");
                //$this->log(json_encode($syncItem));
                unset($syncItem->crm_id);
                unset($syncItem->id);
                try {
                    $result = $this->sugarCrmRest->comunicate('/Accounts/' . $crm_id, 'PUT', $syncItem);
                } catch(SugarCrmRestException $e) {
                    //go ahead with false silently
                    $this->log("REMOTE UPDATE ERROR!!! - " . $e->getMessage());
                    //we must remove crm_id from $cacheItem
                    //create fake result
                    $result = new \stdClass();
                    $result->updateFailure = TRUE;
                }
            }
            else {
                /*
                 * @todo: check if 'crm_export_flag_c' is 0
                 * if "0" check if account exists by P.IVA and if it does update it - if not skip
                 * otherwise create
                 */
                //CREATE
                $this->log("creating remote...");
                //$this->log(json_encode($syncItem));
                unset($syncItem->crm_id);
                unset($syncItem->id);
                try {
                    $result = $this->sugarCrmRest->comunicate('/Accounts', 'POST', $syncItem);
                } catch(SugarCrmRestException $e) {
                    $this->log("REMOTE INSERT ERROR!!! - " . $e->getMessage());
                }
            }
        }
        else {
            $this->log("-----------------------------------------------------------------------------------------");
            $this->log("SKIPPING(ALREADY UP TO DATE): " . $cacheItem->name);
//            $this->log("METODO LAST UPDATE: " . $metodoLastUpdate->format("c"));
//            $this->log("CRM LAST UPDATE: " . $crmLastUpdate->format("c"));
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
    protected function loadRemoteItemId($cacheItem) {
        $crm_id = FALSE;
        $filter = [];
        $fields = [
            "metodo_client_code_imp_c",
            "metodo_supplier_code_imp_c",
            "metodo_client_code_mekit_c",
            "metodo_supplier_code_mekit_c"
        ];

        //identify by the first code
        foreach ($fields as $fieldName) {
            if (isset($cacheItem->$fieldName) && !empty($cacheItem->$fieldName)) {
                $filter[] = [$fieldName => $cacheItem->$fieldName];
                break;
            }
        }

        //try to load 2 of them - if there are more than one it is very BAD!!!
        if (count($filter)) {
            $arguments = [
                "filter" => $filter,
                "max_num" => 2,
                "offset" => 0,
                "fields" => "id",
            ];
            try {
                $result = $this->sugarCrmRest->comunicate('/Accounts/filter', 'GET', $arguments);
            } catch(SugarCrmRestException $e) {
                //go ahead silently with false
            }
            if (isset($result) && isset($result->records)) {
                if (count($result->records) > 1) {
                    //This should never happen!!!
                    throw new \Exception(
                        "There is a multiple correspondence for requested codes!" . json_encode($filter)
                    );
                }
                if (count($result->records) == 1) {
                    /** @var \stdClass $remoteItem */
                    $remoteItem = $result->records[0];
                    //$this->log("FOUND REMOTE ITEM: " . json_encode($remoteItem));
                    $crm_id = $remoteItem->id;
                }
            }
        }
        else {
            //This should never happen!!!
            throw new \Exception("CacheItem does not have usable code!");
        }
        return ($crm_id);
    }


    /**
     * @param \stdClass $localItem
     * @throws \Exception
     */
    protected function saveLocalItemInCache($localItem) {
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
        /** @var array $warnings */
        $warnings = [];

        //control by: PartitaIva
        if (!empty($localItem->PartitaIva) && $localItem->PartitaIva != '00000000000') {
            $filter = [
                'partita_iva_c' => $localItem->PartitaIva,
            ];
            $candidates = $this->cacheDb->loadItems($filter);
            if ($candidates) {
                foreach ($candidates as $candidate) {
                    if ($localItem->CodiceMetodo == $candidate->$remoteFieldNameForCodiceMetodo) {
                        $cachedItem = $candidate;
                        $operation = "update";
                        $identifiedBy = "PIVA + CM";
                        break;
                    }
                }

                if (!$operation) {
                    if ($localItem->CodiceMetodo == $localItem->ClienteDiFatturazione) {
                        foreach ($candidates as $candidate) {
                            if ($candidate->$remoteFieldNameForCodiceMetodo
                                == $candidate->$remoteFieldNameForClienteDiFatturazione
                            ) {
                                $cachedItem = $candidate;
                                $operation = "update";
                                $identifiedBy = "PIVA + (CM===CF)";
                                break;
                            }
                        }
                    }
                }

                if (!$operation) {
                    //$operation = "insert";
                    $identifiedBy = "PIVA";
                }
            }
        }

        //control by: CodiceFiscale
        if (!$operation && !empty($localItem->CodiceFiscale)) {
            $filter = [
                'codice_fiscale_c' => $localItem->CodiceFiscale,
            ];
            $candidates = $this->cacheDb->loadItems($filter);
            if ($candidates) {
                foreach ($candidates as $candidate) {
                    if ($localItem->CodiceMetodo == $candidate->$remoteFieldNameForCodiceMetodo) {
                        $cachedItem = $candidate;
                        $operation = "update";
                        $identifiedBy = "CODFISC + CM";
                        break;
                    }
                }

                if (!$operation) {
                    if ($localItem->CodiceMetodo == $localItem->ClienteDiFatturazione) {
                        foreach ($candidates as $candidate) {
                            if ($candidate->$remoteFieldNameForCodiceMetodo
                                == $candidate->$remoteFieldNameForClienteDiFatturazione
                            ) {
                                $cachedItem = $candidate;
                                $operation = "update";
                                $identifiedBy = "CODFISC + (CM===CF)";
                                break;
                            }
                        }
                    }
                }

                if (!$operation) {
                    //$operation = "insert";
                    $identifiedBy = "CODFISC";
                }
            }
        }

        //control by: Codice Metodo
        if (!$operation) {
            $filter = [
                $remoteFieldNameForCodiceMetodo => $localItem->CodiceMetodo
            ];
            $candidates = $this->cacheDb->loadItems($filter);
            if (count($candidates) > 1) {
                throw new \Exception(
                    "Duplicati per codice metodo(" . $localItem->CodiceMetodo . ") in field_ "
                    . $remoteFieldNameForCodiceMetodo
                );
            }
            if ($candidates) {
                $cachedItem = $candidates[0];
                $operation = "update";
                $identifiedBy = "CM";
            }
            else {
                $operation = "insert";
                $identifiedBy = "NOPE";
            }
        }

        //create item for: update
        if ($operation == "update") {
            $cacheUpdateItem = clone($cachedItem);

            //CHECK FOR BAD DUPLICATES IN METODO
            /*
             * Se identifichiamo un $cachedItem per partitaIva o Codice Fiscale ma il codice metodo attuale($localItem->CodiceMetodo)
             * è diverso dal codice metodo che si trova sul $cachedItem, vuol dire che in Metodo abbiamo più di un anagrafica
             * registrata con la stessa PI/CF
             * Quindi in questo caso invalidiamo il $cachedItem e creiamo nuovo
             */
            if (
                (!empty($cachedItem->$remoteFieldNameForCodiceMetodo)
                 && $localItem->CodiceMetodo != $cachedItem->$remoteFieldNameForCodiceMetodo)
                || (!empty($cachedItem->$remoteFieldNameForClienteDiFatturazione)
                    && $localItem->ClienteDiFatturazione != $cachedItem->$remoteFieldNameForClienteDiFatturazione)
            ) {
                $warnings[] = "-----------------------------------------------------------------------------------------";
                $warnings[] = "RESOLVING CONFLICT[" . $localItem->database . "]($remoteFieldNameForCodiceMetodo): "
                              . $localItem->CodiceMetodo
                              . " -> "
                              . $cachedItem->$remoteFieldNameForCodiceMetodo;
                $warnings[] = $localItem->CodiceMetodo . " = " . $localItem->RagioneSociale;
                $warnings[] = $cachedItem->$remoteFieldNameForCodiceMetodo . " = " . $cachedItem->name;
                $cachedItem = FALSE;
                $operation = "insert";
                $identifiedBy = "CONFLICT";
            }
            else {
                $metodoLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->DataDiModifica);
                $cachedDataDiModifica = new \DateTime($cachedItem->metodo_last_update_time_c);
                if ($metodoLastUpdateTime > $cachedDataDiModifica) {
                    $cacheUpdateItem->metodo_last_update_time_c = $metodoLastUpdateTime->format("c");
                }
            }
        }

        //create item for: insert
        if ($operation == "insert") {
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

        if (!empty($localItem->PartitaIva) && $localItem->PartitaIva != "00000000000") {
            $cacheUpdateItem->partita_iva_c = $localItem->PartitaIva;
        }
        if (!empty($localItem->CodiceFiscale)) {
            $cacheUpdateItem->codice_fiscale_c = $localItem->CodiceFiscale;
        }

        //DECIDE OPERATION
        $operation = ($cachedItem == $cacheUpdateItem) ? "skip" : $operation;

        //add other data on item

        //set it to "0"
        $cacheUpdateItem->crm_export_flag_c = $localItem->CrmExportFlag;


        $cacheUpdateItem->name = $localItem->RagioneSociale;


        if ($operation == "update") {
            $this->log("-----------------------------------------------------------------------------------------");
            $this->log(
                "[" . $localItem->database . "][$operation][$identifiedBy]-"
                . "[" . $localItem->CodiceMetodo . "]"
                . "[" . $localItem->ClienteDiFatturazione . "]"
                . " " . $localItem->RagioneSociale . ""
            );
            $this->log("CACHED: " . json_encode($cachedItem));
            $this->log("UPDATE: " . json_encode($cacheUpdateItem));
            $this->log("-----------------------------------------------------------------------------------------");
        }
        if (!empty($warnings)) {
            foreach ($warnings as $warning) {
                $this->log("WARNING: " . $warning);
            }
        }

        switch ($operation) {
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
                ACFR.CODCONTOFATT AS ClienteDiFatturazione,
                CrmExportFlag = CASE
                     WHEN EXTC.SOGCRM_Esportabile IS NOT NULL THEN EXTC.SOGCRM_Esportabile
                     WHEN EXTF.SOGCRM_Esportabile IS NOT NULL THEN EXTF.SOGCRM_Esportabile
                     ELSE 1 END
                FROM [$database].[dbo].[ANAGRAFICACF] AS ACF
                INNER JOIN [$database].[dbo].[ANAGRAFICARISERVATICF] AS ACFR ON ACF.CODCONTO = ACFR.CODCONTO AND ACFR.ESERCIZIO = (SELECT TOP (1) TE.CODICE FROM [$database].[dbo].[TABESERCIZI] AS TE ORDER BY TE.CODICE DESC)
                LEFT JOIN [$database].dbo.EXTRACLIENTI AS EXTC ON ACF.CODCONTO = EXTC.CODCONTO
                LEFT JOIN [$database].dbo.EXTRAFORNITORI AS EXTF ON ACF.CODCONTO = EXTF.CODCONTO
                ORDER BY ACF.DATAMODIFICA ASC;
                ";

            $this->localItemStatement = $db->prepare($sql);
            $this->localItemStatement->execute();
        }
        $item = $this->localItemStatement->fetch(\PDO::FETCH_OBJ);
        if ($item) {
            $item->CodiceMetodo = trim($item->CodiceMetodo);
            $item->database = $database;
            $item->RagioneSociale = $item->Nome1 . (!empty($item->Nome2) ? ' - ' . $item->Nome2 : '');
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
    protected function getRemoteFieldNameForClienteDiFatturazione($database, $type) {
        switch ($database) {
            case "IMP":
                switch ($type) {
                    case "C":
                        $answer = "metodo_inv_cli_imp_c";
                        break;
                    case "F":
                        $answer = "metodo_inv_sup_imp_c";
                        break;
                    default:
                        throw new \Exception("Local item needs to have Tipologia C|F!");
                }
                break;
            case "MEKIT":
                switch ($type) {
                    case "C":
                        $answer = "metodo_inv_cli_mekit_c";
                        break;
                    case "F":
                        $answer = "metodo_inv_sup_mekit_c";
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