<?php
/**
 * Created by Adam Jakab.
 * Date: 05/01/16
 * Time: 14.34
 */

namespace Mekit\Sync\Metodo;

use Mekit\Console\Configuration;
use Mekit\DbCache\OfferCache;
use Mekit\DbCache\OfferLineCache;
use Mekit\SugarCrm\Rest\SugarCrmRest;
use Mekit\SugarCrm\Rest\SugarCrmRestException;
use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;
use Monolog\Logger;

class OfferData extends Sync implements SyncInterface {
    /** @var callable */
    protected $logger;

    /** @var SugarCrmRest */
    protected $sugarCrmRest;

    /** @var  OfferCache */
    protected $offerCacheDb;

    /** @var OfferLineCache */
    protected $offerLineCacheDb;

    /** @var  \PDOStatement */
    protected $localItemStatement;

    /** @var  \PDOStatement */
    protected $localItemStatement2;

    /** @var array */
    protected $counters = [];

    /**
     * @param callable $logger
     */
    public function __construct($logger) {
        parent::__construct($logger);
        $this->offerCacheDb = new OfferCache('Offers', $logger);
        $this->offerLineCacheDb = new OfferLineCache('Offers_Lines', $logger);
        $this->sugarCrmRest = new SugarCrmRest();
    }

    /**
     * @param array $options
     */
    public function execute($options) {
        //$this->log("EXECUTING..." . json_encode($options));
        if (isset($options["delete-cache"]) && $options["delete-cache"]) {
            $this->offerCacheDb->removeAll();
            $this->offerLineCacheDb->removeAll();
        }

        if (isset($options["invalidate-cache"]) && $options["invalidate-cache"]) {
            $this->offerCacheDb->invalidateAll(TRUE, TRUE);
            $this->offerLineCacheDb->invalidateAll(TRUE, TRUE);
        }

        if (isset($options["invalidate-local-cache"]) && $options["invalidate-local-cache"]) {
            $this->offerCacheDb->invalidateAll(TRUE, FALSE);
            $this->offerLineCacheDb->invalidateAll(TRUE, FALSE);
        }

        if (isset($options["invalidate-remote-cache"]) && $options["invalidate-remote-cache"]) {
            $this->offerCacheDb->invalidateAll(FALSE, TRUE);
            $this->offerLineCacheDb->invalidateAll(FALSE, TRUE);
        }

        if (isset($options["update-cache"]) && $options["update-cache"]) {
            $this->updateLocalCache();
        }

        if (isset($options["update-remote"]) && $options["update-remote"]) {
            $this->updateRemoteFromCache();
        }
    }

    /**
     * get data from Metodo and store it in local cache
     */
    protected function updateLocalCache() {
        $FORCE_LIMIT = FALSE;
        $this->log("updating local cache(offers)...");
        $this->counters["cache"]["index"] = 0;
        foreach (["MEKIT", "IMP"] as $database) {
            while ($localItem = $this->getNextOffer($database)) {
                if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counters["cache"]["index"] >= $FORCE_LIMIT) {
                    break;
                }
                $this->counters["cache"]["index"]++;
                $this->saveOfferInCache($localItem);
            }
        }
        //
        $this->log("updating local cache(offer lines)...");
        $this->counters["cache"]["index"] = 0;
        foreach (["MEKIT", "IMP"] as $database) {
            while ($localItem = $this->getNextOfferLine($database)) {
                if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counters["cache"]["index"] >= $FORCE_LIMIT) {
                    break;
                }
                $this->counters["cache"]["index"]++;
                $this->saveOfferLineInCache($localItem);
            }
        }
    }


    protected function updateRemoteFromCache() {
        //OFFERS
        $FORCE_LIMIT = FALSE;
        $this->log("updating remote(offers)...");
        $this->offerCacheDb->resetItemWalker();
        $this->counters["remote"]["index"] = 0;
        while ($cacheItem = $this->offerCacheDb->getNextItem('metodo_last_update_time', $orderDir = 'ASC')) {
            if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counters["remote"]["index"] >= $FORCE_LIMIT) {
                $this->offerCacheDb->resetItemWalker();
                break;
            }
            $this->counters["remote"]["index"]++;
            $remoteItem = $this->saveOfferOnRemote($cacheItem);
            $this->storeCrmIdForCachedOffer($cacheItem, $remoteItem);
        }

        //OFFER LINES
        $FORCE_LIMIT = FALSE;
        $this->log("updating remote(offer lines)...");
        $this->offerLineCacheDb->resetItemWalker();
        $this->counters["remote"]["index"] = 0;
        while ($cacheItem = $this->offerLineCacheDb->getNextItem('metodo_last_update_time', $orderDir = 'ASC')) {
            if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counters["remote"]["index"] >= $FORCE_LIMIT) {
                $this->offerLineCacheDb->resetItemWalker();
                break;
            }
            $this->counters["remote"]["index"]++;
            $remoteItem = $this->saveOfferLineOnRemote($cacheItem);
            $this->storeCrmIdForCachedOfferLine($cacheItem, $remoteItem);
        }
    }



    //------------------------------------------------------------------------------------------------------------REMOTE
    /**
     * @param \stdClass $cacheItem
     * @param \stdClass $remoteItem
     * @return \stdClass|bool
     */
    protected function relateOfferLineToOfferOnRemote($cacheItem, $remoteItem) {
        $result = FALSE;

        if ($remoteItem && !empty($remoteItem->id)) {
            $offerLineCrmId = $remoteItem->id;

            $this->log(
                "-----------------------------------------------------------------------------------------"
                . $this->counters["remote"]["index"]
            );

            //get cached offer by id
            $filter = [
                'id' => $cacheItem->offer_id,
            ];
            $candidates = $this->offerCacheDb->loadItems($filter);
            if ($candidates && count($candidates) == 1) {
                $cachedOffer = $candidates[0];
                $offerCrmId = $cachedOffer->crm_id;
                if ($offerCrmId) {
                    $this->log("Creating Offer($offerCrmId) - OfferLine($offerLineCrmId) relationship...");


                    try {
                        $linkData = [
                            "link_name" => 'mkt_offers_mkt_offer_lines',
                            "ids" => [$offerLineCrmId]
                        ];
                        $result = $this->sugarCrmRest->comunicate(
                            '/mkt_Offers/' . $offerCrmId . '/link', 'POST', $linkData
                        );
                        $this->log("LINKED: " . json_encode($result));
                        $result = TRUE;
                    } catch(SugarCrmRestException $e) {
                        //go ahead with false silently
                        $this->log("REMOTE RELATIONSHIP ERROR!!! - " . $e->getMessage());
                    }
                }
            }
        }
        return $result;

    }

    /**
     * @param \stdClass $cacheItem
     * @return \stdClass|bool
     */
    protected function saveOfferLineOnRemote($cacheItem) {
        $remoteItem = FALSE;
        $ISO = 'Y-m-d\TH:i:sO';
        $metodoLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->metodo_last_update_time);
        $crmLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->crm_last_update_time);

        if ($metodoLastUpdate > $crmLastUpdate) {
            $this->log(
                "-----------------------------------------------------------------------------------------"
                . $this->counters["remote"]["index"]
            );

            try {
                $crm_id = $this->loadRemoteOfferLineId($cacheItem);
            } catch(\Exception $e) {
                $this->log("CANNOT LOAD ID FROM CRM - UPDATE WILL BE SKIPPED: " . $e->getMessage());
                return $remoteItem;
            }

            $syncItem = clone($cacheItem);

            //modify sync item here

            //unset data
            unset($syncItem->crm_id);
            unset($syncItem->id);

            $this->log("CRM SYNC ITEM: " . json_encode($syncItem));

            if ($crm_id) {
                //UPDATE
                $this->log("updating remote($crm_id)...");
                try {
                    $remoteItem = $this->sugarCrmRest->comunicate('/mkt_Offer_Lines/' . $crm_id, 'PUT', $syncItem);
                } catch(SugarCrmRestException $e) {
                    //go ahead with false silently
                    $this->log("REMOTE UPDATE ERROR!!! - " . $e->getMessage());
                    //we must remove crm_id from $cacheItem
                    //create fake result
                    $remoteItem = new \stdClass();
                    $remoteItem->updateFailure = TRUE;
                }
            }
            else {
                //CREATE
                $this->log("creating remote...");
                try {
                    $remoteItem = $this->sugarCrmRest->comunicate('/mkt_Offer_Lines', 'POST', $syncItem);
                } catch(SugarCrmRestException $e) {
                    $this->log("REMOTE INSERT ERROR!!! - " . $e->getMessage());
                }
            }
        }

        if ($remoteItem) {
            $this->relateOfferLineToOfferOnRemote($cacheItem, $remoteItem);
        }

        return $remoteItem;
    }

    /**
     * @param \stdClass $cacheItem
     * @return \stdClass|bool
     */
    protected function saveOfferOnRemote($cacheItem) {
        $remoteItem = FALSE;
        $ISO = 'Y-m-d\TH:i:sO';
        $metodoLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->metodo_last_update_time);
        $crmLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->crm_last_update_time);

        if ($metodoLastUpdate > $crmLastUpdate) {
            $this->log(
                "-----------------------------------------------------------------------------------------"
                . $this->counters["remote"]["index"]
            );

            try {
                $crm_id = $this->loadRemoteOfferId($cacheItem);
            } catch(\Exception $e) {
                $this->log("CANNOT LOAD ID FROM CRM - UPDATE WILL BE SKIPPED: " . $e->getMessage());
                return $remoteItem;
            }

            $syncItem = clone($cacheItem);

            //modify sync item here
            $dataDoc = \DateTime::createFromFormat('Y-m-d H:i:s.u', $syncItem->data_doc);
            $syncItem->data_doc = $dataDoc->format("c");


            //unset data
            unset($syncItem->crm_id);
            unset($syncItem->id);

            $this->log("CRM SYNC ITEM: " . json_encode($syncItem));

            if ($crm_id) {
                //UPDATE
                $this->log("updating remote($crm_id)...");
                try {
                    $remoteItem = $this->sugarCrmRest->comunicate('/mkt_Offers/' . $crm_id, 'PUT', $syncItem);
                } catch(SugarCrmRestException $e) {
                    //go ahead with false silently
                    $this->log("REMOTE UPDATE ERROR!!! - " . $e->getMessage());
                    //we must remove crm_id from $cacheItem
                    //create fake result
                    $remoteItem = new \stdClass();
                    $remoteItem->updateFailure = TRUE;
                }
            }
            else {
                //CREATE
                $this->log("creating remote...");
                try {
                    $remoteItem = $this->sugarCrmRest->comunicate('/mkt_Offers', 'POST', $syncItem);
                } catch(SugarCrmRestException $e) {
                    $this->log("REMOTE INSERT ERROR!!! - " . $e->getMessage());
                }
            }
        }
        return $remoteItem;
    }

    /**
     * They would be recreated all over again
     * @param \stdClass $cacheItem
     * @return string|bool
     * @throws \Exception
     */
    protected function loadRemoteOfferLineId($cacheItem) {
        $crm_id = FALSE;
        $filter = [];

        $filter[] = [
            'database_metodo' => $cacheItem->database_metodo,
            'id_line' => $cacheItem->id_line
        ];

        //try to load 2 of them - if there are more than one we do not know which one to update
        $arguments = [
            "filter" => $filter,
            "max_num" => 2,
            "offset" => 0,
            "fields" => "id",
        ];

        $result = $this->sugarCrmRest->comunicate('/mkt_Offer_Lines/filter', 'GET', $arguments);

        if (isset($result) && isset($result->records)) {

            $this->log("IDSEARCH(" . json_encode($filter) . "): " . json_encode($result));

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
        return ($crm_id);
    }

    /**
     * They would be recreated all over again
     * @param \stdClass $cacheItem
     * @return string|bool
     * @throws \Exception
     */
    protected function loadRemoteOfferId($cacheItem) {
        $crm_id = FALSE;
        $filter = [];

        $filter[] = [
            'database_metodo' => $cacheItem->database_metodo,
            'id_head' => $cacheItem->id_head
        ];

        //try to load 2 of them - if there are more than one we do not know which one to update
        $arguments = [
            "filter" => $filter,
            "max_num" => 2,
            "offset" => 0,
            "fields" => "id",
        ];

        $result = $this->sugarCrmRest->comunicate('/mkt_Offers/filter', 'GET', $arguments);

        if (isset($result) && isset($result->records)) {

            $this->log("IDSEARCH(" . json_encode($filter) . "): " . json_encode($result));

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
        return ($crm_id);
    }

    /**
     * @param \stdClass $cacheItem
     * @param \stdClass $remoteItem
     */
    protected function storeCrmIdForCachedOfferLine($cacheItem, $remoteItem) {
        if ($remoteItem) {
            $cacheUpdateItem = new \stdClass();
            $cacheUpdateItem->id = $cacheItem->id;
            if (isset($remoteItem->updateFailure) && $remoteItem->updateFailure) {
                //we must remove crm_id and reset crm_last_update_time on $cacheItem
                $cacheUpdateItem->crm_id = NULL;
                $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
                $cacheUpdateItem->crm_last_update_time = $oldDate->format("c");
            }
            else {
                $cacheUpdateItem->crm_id = $remoteItem->id;
                $now = new \DateTime();
                $cacheUpdateItem->crm_last_update_time = $now->format("c");
            }
            $this->offerLineCacheDb->updateItem($cacheUpdateItem);
        }
    }

    /**
     * @param \stdClass $cacheItem
     * @param \stdClass $remoteItem
     */
    protected function storeCrmIdForCachedOffer($cacheItem, $remoteItem) {
        if ($remoteItem) {
            $cacheUpdateItem = new \stdClass();
            $cacheUpdateItem->id = $cacheItem->id;
            if (isset($remoteItem->updateFailure) && $remoteItem->updateFailure) {
                //we must remove crm_id and reset crm_last_update_time on $cacheItem
                $cacheUpdateItem->crm_id = NULL;
                $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
                $cacheUpdateItem->crm_last_update_time = $oldDate->format("c");
            }
            else {
                $cacheUpdateItem->crm_id = $remoteItem->id;
                $now = new \DateTime();
                $cacheUpdateItem->crm_last_update_time = $now->format("c");
            }
            $this->offerCacheDb->updateItem($cacheUpdateItem);
        }
    }

    //-------------------------------------------------------------------------------------------------------------CACHE
    /**
     * @param \stdClass $localItem
     * @throws \Exception
     */
    protected function saveOfferLineInCache($localItem) {
        /** @var array|bool $operation */
        $operation = FALSE;

        /** @var \stdClass $cachedItem */
        $cachedItem = FALSE;

        /** @var \stdClass $updateItem */
        $updateItem = FALSE;

        /** @var array $warnings */
        $warnings = [];

        /** @var string $itemDb IMP|MEKIT */
        $itemDb = $localItem->database_metodo;


        $this->log(
            "-----------------------------------------------------------------------------------------"
            . $this->counters["cache"]["index"]
        );

        //get by id_head
        $filter = [
            'database_metodo' => $itemDb,
            'id_line' => $localItem->id_line,
        ];
        $candidates = $this->offerLineCacheDb->loadItems($filter);
        if ($candidates && count($candidates)) {
            if (count($candidates) > 1) {
                throw new \Exception("Multiple Line Id found for db!");
            }
            $cachedItem = $candidates[0];
            $updateItem = clone($localItem);
            $updateItem->id = $cachedItem->id;
            $operation = 'update';
        }

        //not there - create new
        if (!$cachedItem) {
            $updateItem = $this->generateNewOfferLineObject($localItem);
            $operation = 'insert';
        }

        $metodoLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->metodo_last_update_time);
        $updateItemLastUpdateTime = new \DateTime($updateItem->metodo_last_update_time);
        if ($metodoLastUpdateTime > $updateItemLastUpdateTime) {
            $updateItem->metodo_last_update_time = $metodoLastUpdateTime->format("c");
        }
        else {
            $operation = "skip";
        }

        //DECIDE OPERATION
        //$operation = ($cachedItem == $updateItem) ? "skip" : $operation;


        //LOG
        if ($operation != "skip") {
            $this->log("[" . $itemDb . "][" . json_encode($operation) . "]:");
            $this->log("LOCAL: " . json_encode($localItem));
            $this->log("CACHED: " . json_encode($cachedItem));
            $this->log("UPDATE: " . json_encode($updateItem));

        }
        if (!empty($warnings)) {
            foreach ($warnings as $warning) {
                $this->log("WARNING: " . $warning);
            }
        }

        //CHECK
        if (!isset($operation)) {
            throw new \Exception("operation NOT SET!");
        }

        //INSERT / UPDATE OFFER
        switch ($operation) {
            case "insert":
                $this->offerLineCacheDb->addItem($updateItem);
                break;
            case "update":
                $this->offerLineCacheDb->updateItem($updateItem);
                break;
            case "skip":
                break;
            default:
                throw new \Exception("Code operation(" . $operation . ") is not implemented!");
        }
    }

    /**
     * @param \stdClass $localItem
     * @throws \Exception
     */
    protected function saveOfferInCache($localItem) {
        /** @var array|bool $operation */
        $operation = FALSE;

        /** @var \stdClass $cachedItem */
        $cachedItem = FALSE;

        /** @var \stdClass $updateItem */
        $updateItem = FALSE;

        /** @var array $warnings */
        $warnings = [];

        /** @var string $itemDb IMP|MEKIT */
        $itemDb = $localItem->database_metodo;


        $this->log(
            "-----------------------------------------------------------------------------------------"
            . $this->counters["cache"]["index"]
        );

        //get by id_head
        $filter = [
            'database_metodo' => $itemDb,
            'id_head' => $localItem->id_head,
        ];
        $candidates = $this->offerCacheDb->loadItems($filter);
        if ($candidates && count($candidates)) {
            if (count($candidates) > 1) {
                throw new \Exception("Multiple Head Id found for db!");
            }
            $cachedItem = $candidates[0];
            $updateItem = clone($localItem);
            $updateItem->id = $cachedItem->id;
            $updateItem->metodo_last_update_time = $cachedItem->metodo_last_update_time;
            $operation = 'update';
        }

        //not there - create new
        if (!$cachedItem) {
            $updateItem = $this->generateNewOfferObject($localItem);
            $operation = 'insert';
        }

        $metodoLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->metodo_last_update_time);
        $updateItemLastUpdateTime = new \DateTime($updateItem->metodo_last_update_time);
        if ($metodoLastUpdateTime > $updateItemLastUpdateTime) {
            $updateItem->metodo_last_update_time = $metodoLastUpdateTime->format("c");
        }
        else {
            $operation = "skip";
        }

        //DECIDE OPERATION
        //$operation = ($cachedItem == $updateItem) ? "skip" : $operation;


        //LOG
        if ($operation != "skip") {
            $this->log("[" . $itemDb . "][" . json_encode($operation) . "]:");
            $this->log("LOCAL: " . json_encode($localItem));
            $this->log("CACHED: " . json_encode($cachedItem));
            $this->log("UPDATE: " . json_encode($updateItem));

        }
        if (!empty($warnings)) {
            foreach ($warnings as $warning) {
                $this->log("WARNING: " . $warning);
            }
        }

        //CHECK
        if (!isset($operation)) {
            throw new \Exception("operation NOT SET!");
        }

        //INSERT / UPDATE OFFER
        switch ($operation) {
            case "insert":
                $this->offerCacheDb->addItem($updateItem);
                break;
            case "update":
                $this->offerCacheDb->updateItem($updateItem);
                break;
            case "skip":
                break;
            default:
                throw new \Exception("Code operation(" . $operation . ") is not implemented!");
        }
    }


    /**
     * @param \stdClass $localItem
     * @return \stdClass
     */
    protected function generateNewOfferObject($localItem) {
        $offer = clone($localItem);
        $offer->id = md5(json_encode($localItem) . microtime(TRUE));
        $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
        $offer->metodo_last_update_time = $oldDate->format("c");
        $offer->crm_last_update_time = $oldDate->format("c");
        return $offer;
    }

    /**
     * @param \stdClass $localItem
     * @return \stdClass
     * @throws \Exception
     */
    protected function generateNewOfferLineObject($localItem) {
        $offerLine = clone($localItem);
        $offerLine->id = md5(json_encode($localItem) . microtime(TRUE));
        $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
        $offerLine->metodo_last_update_time = $oldDate->format("c");
        $offerLine->crm_last_update_time = $oldDate->format("c");
        // get offer_id
        $filter = [
            'database_metodo' => $localItem->database_metodo,
            'id_head' => $localItem->id_head,
        ];
        $candidates = $this->offerCacheDb->loadItems($filter);
        if ($candidates && count($candidates)) {
            if (count($candidates) > 1) {
                throw new \Exception("Multiple Head Id found for db!");
            }
            $candidate = $candidates[0];
            $offerLine->offer_id = $candidate->id;
        }
        //
        return $offerLine;
    }


    /**
     * @param string $database IMP|MEKIT
     * @return bool|\stdClass
     */
    protected function getNextOffer($database) {
        if (!$this->localItemStatement) {
            $db = Configuration::getDatabaseConnection("SERVER2K8");
            $sql = "SELECT
                TD.PROGRESSIVO AS id_head,
                TD.NUMERODOC AS document_number,
                TD.DATADOC AS data_doc,
                TD.CODCLIFOR AS cod_c_f,
                TD.CODAGENTE1 AS " . strtolower($database) . "_agent_code,
                ISNULL(TP.DESCRIZIONE, '') AS dsc_payment,
                TD.TOTIMPONIBILEEURO * TD.SEGNO AS tot_imponibile_euro,
                TD.TOTIMPOSTAEURO * TD.SEGNO AS tot_imposta_euro,
                TD.TOTDOCUMENTOEURO * TD.SEGNO AS tot_documento_euro,
                TD.DATAMODIFICA AS metodo_last_update_time
                FROM [${database}].[dbo].[TESTEDOCUMENTI] AS TD
                LEFT OUTER JOIN [${database}].[dbo].[TABPAGAMENTI] AS TP ON TD.CODPAGAMENTO = TP.CODICE
                WHERE TIPODOC = 'OFC';
            ";
            $this->localItemStatement = $db->prepare($sql);
            $this->localItemStatement->execute();
        }
        $item = $this->localItemStatement->fetch(\PDO::FETCH_OBJ);
        if ($item) {
            $item->database_metodo = $database;
        }
        else {
            $this->localItemStatement = NULL;
        }
        return $item;
    }

    /**
     * @param string $database IMP|MEKIT
     * @return bool|\stdClass
     */
    protected function getNextOfferLine($database) {
        if (!$this->localItemStatement) {
            $db = Configuration::getDatabaseConnection("SERVER2K8");
            $sql = "SELECT
                TD.PROGRESSIVO AS id_head,
                CONCAT(TD.PROGRESSIVO, '-', RD.IDRIGA) AS id_line,
                RD.POSIZIONE AS line_order,
                RD.CODART AS article_code,
                RD.DESCRIZIONEART AS article_description,
                RD.NUMLISTINO AS price_list_number,
                (CASE WHEN RD.TIPORIGA = 'V' THEN 0 ELSE RD.QTAPREZZO * TD.SEGNO END) AS quantity,
                RD.UMPREZZO AS measure_unit,
                RD.TOTLORDORIGAEURO * TD.SEGNO AS gross_total,
                RD.TOTNETTORIGAEURO * TD.SEGNO AS net_total,
                CASE WHEN NULLIF(RD.CODART, '') IS NOT NULL THEN
                (CASE WHEN RD.TIPORIGA = 'V' THEN 0 ELSE RD.QTAPREZZO * TD.SEGNO END) * ART.PREZZOEURO
                ELSE 0
                END AS net_total_listino_42,
                RD.DATAMODIFICA AS metodo_last_update_time
                FROM
                [${database}].[dbo].[TESTEDOCUMENTI] AS TD
                INNER JOIN [${database}].[dbo].[RIGHEDOCUMENTI] AS RD ON TD.PROGRESSIVO = RD.IDTESTA
                LEFT OUTER JOIN [${database}].[dbo].[LISTINIARTICOLI] AS ART ON RD.CODART = ART.CODART AND ART.NRLISTINO = 42
                WHERE TD.TIPODOC = 'OFC'
                ORDER BY TD.PROGRESSIVO;
            ";
            $this->localItemStatement = $db->prepare($sql);
            $this->localItemStatement->execute();
        }
        $item = $this->localItemStatement->fetch(\PDO::FETCH_OBJ);
        if ($item) {
            $item->database_metodo = $database;
        }
        else {
            $this->localItemStatement = NULL;
        }
        return $item;
    }
}