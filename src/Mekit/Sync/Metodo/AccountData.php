<?php
/**
 * Created by Adam Jakab.
 * Date: 01/12/15
 * Time: 11.32
 */

namespace Mekit\Sync\Metodo;

use Mekit\Console\Configuration;
use Mekit\DbCache\AccountCache;
use Mekit\SugarCrm\Rest\SugarCrmRest;
use Mekit\SugarCrm\Rest\SugarCrmRestException;
use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;
use Monolog\Logger;

class AccountData extends Sync implements SyncInterface {
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
    public function __construct($logger) {
        parent::__construct($logger);
        $this->cacheDb = new AccountCache('Account', $logger);
        $this->sugarCrmRest = new SugarCrmRest();
    }

    /**
     * @param array $options
     */
    public function execute($options) {
        //$this->log("EXECUTING..." . json_encode($options));
        if (isset($options["delete-cache"]) && $options["delete-cache"]) {
            $this->cacheDb->removeAll();
        }

        if (isset($options["invalidate-cache"]) && $options["invalidate-cache"]) {
            $this->cacheDb->invalidateAll(TRUE, TRUE);
        }

        if (isset($options["invalidate-local-cache"]) && $options["invalidate-local-cache"]) {
            $this->cacheDb->invalidateAll(TRUE, FALSE);
        }

        if (isset($options["invalidate-remote-cache"]) && $options["invalidate-remote-cache"]) {
            $this->cacheDb->invalidateAll(FALSE, TRUE);
        }

        if (isset($options["update-cache"]) && $options["update-cache"]) {
            $this->updateLocalCache();
        }

        if (isset($options["update-remote"]) && $options["update-remote"]) {
            $this->updateRemoteFromCache();
        }
    }

    protected function updateLocalCache() {
        $this->log("updating local cache...");
        $this->counters["cache"]["index"] = 0;
        foreach (["MEKIT", "IMP"] as $database) {
            while ($localItem = $this->getNextLocalItem($database)) {
                $this->counters["cache"]["index"]++;
                $this->saveLocalItemInCache($localItem);
            }
        }
    }

    protected function updateRemoteFromCache() {
        $this->log("updating remote...");
        $this->cacheDb->resetItemWalker();
        $this->counters["remote"]["index"] = 0;
        while ($cacheItem = $this->cacheDb->getNextItem()) {
            $this->counters["remote"]["index"]++;
            $remoteItem = $this->saveRemoteItem($cacheItem);
            $this->storeCrmIdForCachedItem($cacheItem, $remoteItem);
            /*
            $this->log("REMOTE: " . json_encode($remoteItem));
            if($this->counters["remote"]["index"] > 1) {
                break;
            }*/
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
                $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
                $cacheUpdateItem->crm_last_update_time_c = $oldDate->format("c");
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
            $this->log(
                "-----------------------------------------------------------------------------------------"
                . $this->counters["remote"]["index"]
            );

            try {
                $crm_id = $this->loadRemoteItemId($cacheItem);
            } catch(\Exception $e) {
                $this->log("CANNOT LOAD ID FROM CRM - UPDATE WILL BE SKIPPED");
                return $result;
            }

            $syncItem = clone($cacheItem);

            //add payload to syncItem
            $payload = $this->getLocalItemPayload($cacheItem);
            if ($payload) {
                foreach ($payload as $key => $payloadData) {
                    //special cases
                    if ($key == "email") {
                        $syncItem->$key = [
                            [
                                "email_address" => $payloadData,
                                "invalid_email" => FALSE,
                                "opt_out" => FALSE,
                                "primary_address" => FALSE,
                                "reply_to_address" => FALSE
                            ]
                        ];
                        $syncItem->email1 = $payloadData;
                    }
                    else {
                        $syncItem->$key = $payloadData;
                    }
                }
            }
            //add special data
            $syncItem->profiling_c = FALSE;//"Da profilare"


            $this->log("CRM SYNC ITEM: " . json_encode($syncItem));



            //UPDATE
            if ($crm_id) {
                $this->log("updating remote($crm_id): " . $syncItem->name);
                unset($syncItem->crm_id);
                unset($syncItem->id);
                try {
                    $result = $this->sugarCrmRest->comunicate('/Accounts/' . $crm_id, 'PUT', $syncItem);
                    $this->log("UPDATE REMOTE RESULT: " . json_encode($result));
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
                $this->log("creating remote(" . $syncItem->name . ")...");
                unset($syncItem->crm_id);
                unset($syncItem->id);
                try {
                    $result = $this->sugarCrmRest->comunicate('/Accounts', 'POST', $syncItem);
                    $this->log("CREATE REMOTE RESULT: " . json_encode($result));
                } catch(SugarCrmRestException $e) {
                    $this->log("REMOTE INSERT ERROR!!! - " . $e->getMessage());
                }
            }
        }
        else {
            //$this->log("SKIPPING(ALREADY UP TO DATE): " . $cacheItem->name);
            //$this->log("METODO LAST UPDATE: " . $metodoLastUpdate->format("c"));
            //$this->log("CRM LAST UPDATE: " . $crmLastUpdate->format("c"));
        }
        return $result;
    }

    /**
     * @param \stdClass $cacheItem
     * @return array
     * @throws \Exception
     */
    protected function getNonEmptyMetodoCodeFieldNamesFromCacheItem($cacheItem) {
        $answer = [];
        $fields = [
            "metodo_client_code_imp_c",
            "metodo_supplier_code_imp_c",
            "metodo_client_code_mekit_c",
            "metodo_supplier_code_mekit_c"
        ];
        foreach ($fields as $fieldName) {
            if (isset($cacheItem->$fieldName) && !empty($cacheItem->$fieldName)) {
                $answer[] = $fieldName;
            }
        }
        if (!count($answer)) {
            throw new \Exception("No non-empty field names can be found on cache item!");
        }
        return $answer;
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

        //identify by codice metodo - the first one found
        $fieldNames = $this->getNonEmptyMetodoCodeFieldNamesFromCacheItem($cacheItem);
        $fieldName = $fieldNames[0];
        $filter[] = [$fieldName => $cacheItem->$fieldName];

        //try to load 2 of them - if there are more than one it is very BAD!!!
        if (count($filter)) {
            $arguments = [
                "filter" => $filter,
                "max_num" => 2,
                "offset" => 0,
                "fields" => "id",
            ];

            $result = $this->sugarCrmRest->comunicate('/Accounts/filter', 'GET', $arguments);

            if (isset($result) && isset($result->records)) {
                if (count($result->records) > 1) {
                    //This should never happen!!!
                    $this->log(str_repeat("-", 120));
                    $this->log(str_repeat("-", 120));
                    $this->log(str_repeat("-", 120));
                    $this->log(
                        "There is a multiple correspondence for requested codes!"
                        . json_encode($filter), Logger::ERROR, $result->records
                    );
                    $this->log("RESULTS: " . json_encode($result->records));
                    $this->log(str_repeat("-", 120));
                    $this->log(str_repeat("-", 120));
                    $this->log(str_repeat("-", 120));
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
        $cacheUpdateItem->crm_export_flag_c = $localItem->CrmExportFlag;
        $cacheUpdateItem->name = $localItem->RagioneSociale;

        if ($operation != "skip") {
            $this->log(
                "-----------------------------------------------------------------------------------------"
                . $this->counters["cache"]["index"]
            );
            $this->log(
                "[" . $localItem->database . "][$operation][$identifiedBy]-"
                . "[" . $localItem->CodiceMetodo . "]"
                . "[" . $localItem->ClienteDiFatturazione . "]"
                . " " . $localItem->RagioneSociale . ""
            );
            $this->log("CACHED: " . json_encode($cachedItem));
            $this->log("UPDATE: " . json_encode($cacheUpdateItem));
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
     * @param $cacheItem
     * @return array|bool
     * @throws \Exception
     */
    protected function getLocalItemPayload($cacheItem) {
        $answer = FALSE;
        $db = Configuration::getDatabaseConnection("SERVER2K8");
        $fieldNames = $this->getNonEmptyMetodoCodeFieldNamesFromCacheItem($cacheItem);
        $databases = ["IMP", "MEKIT"];
        $items = [];
        foreach ($databases as $database) {
            $metodoCodes = [];
            foreach ($fieldNames as $fieldName) {
                if (preg_match("#^metodo_(client|supplier)_code_" . strtolower($database) . "_c$#", $fieldName)) {
                    $metodoCodes[] = "'" . $cacheItem->$fieldName . "'";
                }
            }
            if (count($metodoCodes)) {
                $sql = "SELECT
                    ACF.DATAMODIFICA AS last_updated_at,
                    ACF.INDIRIZZO AS billing_address_street,
                    ACF.CAP AS billing_address_postalcode,
                    ACF.LOCALITA AS billing_address_city,
                    ACF.PROVINCIA AS billing_address_state,
                    ACF.CODICEISO AS billing_address_country,
                    ACF.TELEFONO AS phone_office,
                    ACF.FAX AS phone_fax,
                    ACF.TELEX AS email,
                    ACF.INDIRIZZOINTERNET AS website,
                    ACF.NOTE AS metodo_notes_" . strtolower($database) . "_c,
                    ACFR.CODAGENTE1 AS " . strtolower($database) . "_agent_code_c
                    FROM [$database].[dbo].[ANAGRAFICACF] AS ACF
                    INNER JOIN [$database].[dbo].[ANAGRAFICARISERVATICF] AS ACFR ON ACF.CODCONTO = ACFR.CODCONTO AND ACFR.ESERCIZIO = (SELECT TOP (1) TE.CODICE FROM [$database].[dbo].[TABESERCIZI] AS TE ORDER BY TE.CODICE DESC)
                    WHERE ACF.CODCONTO IN (" . implode(",", $metodoCodes) . ")
                    ";
                $statement = $db->prepare($sql);
                $statement->execute();
                $itemList = $statement->fetchAll(\PDO::FETCH_ASSOC);
                if (count($itemList)) {
                    $items = array_merge($items, $itemList);
                }
            }
        }

        if (count($items)) {
            $answer = [];
            //convert to date
            foreach ($items as &$item) {
                $item["last_updated_at"] = \DateTime::createFromFormat('Y-m-d H:i:s.u', $item["last_updated_at"]);
            }

            //Sort by last_updated_at date ascending
            usort(
                $items, function ($item1, $item2) {
                if ($item1['last_updated_at'] == $item2['last_updated_at']) {
                    return 0;
                }
                return ($item1['last_updated_at'] > $item2['last_updated_at']) ? 1 : -1;
            }
            );

            //merge data into single data
            foreach ($items as &$item) {
                unset($item["last_updated_at"]);//no need for this anymore
                foreach ($item as $itemKey => &$itemData) {
                    if (!empty(trim($itemData))) {
                        $itemData = trim($itemData);
                        //special cases
                        if ($itemKey == "billing_address_country") {
                            $itemData = ($itemData == "IT" ? "ITALIA" : $itemData);
                        }
                    }
                    else {
                        unset($item[$itemKey]);
                    }
                }
                $answer = array_merge($answer, $item);
            }

            if (count($items) > 1) {
//                $this->log("-------------------------------------------------------------------------------------------------");
//                $this->log("MERGED MULTIPLE PAYLOAD ITEMS(SORTED BY DATE): " . json_encode($items));
//                $this->log("MERGE RESULT: " . json_encode($answer));
            }
        }
        return $answer;
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
     * @param string $type
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
}