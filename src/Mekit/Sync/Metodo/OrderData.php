<?php
/**
 * Created by Adam Jakab.
 * Date: 05/01/16
 * Time: 14.34
 */

namespace Mekit\Sync\Metodo;

use Mekit\Console\Configuration;
use Mekit\DbCache\ContactCache;
use Mekit\DbCache\ContactCodesCache;
use Mekit\DbCache\OrderCache;
use Mekit\DbCache\OrderLineCache;
use Mekit\SugarCrm\Rest\SugarCrmRest;
use Mekit\SugarCrm\Rest\SugarCrmRestException;
use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;
use Monolog\Logger;

class OrderData extends Sync implements SyncInterface {
    /** @var callable */
    protected $logger;

    /** @var SugarCrmRest */
    protected $sugarCrmRest;

    /** @var  OrderCache */
    protected $orderCacheDb;

    /** @var OrderLineCache */
    protected $orderLineCacheDb;

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
        $this->orderCacheDb = new OrderCache('Orders', $logger);
        $this->orderLineCacheDb = new OrderLineCache('Orders_Lines', $logger);
        $this->sugarCrmRest = new SugarCrmRest();
    }

    /**
     * @param array $options
     */
    public function execute($options) {
        //$this->log("EXECUTING..." . json_encode($options));
        if (isset($options["delete-cache"]) && $options["delete-cache"]) {
            $this->orderCacheDb->removeAll();
            $this->orderLineCacheDb->removeAll();
        }

        if (isset($options["invalidate-cache"]) && $options["invalidate-cache"]) {
            $this->orderCacheDb->invalidateAll(TRUE, TRUE);
            $this->orderLineCacheDb->invalidateAll(TRUE, TRUE);
        }

        if (isset($options["invalidate-local-cache"]) && $options["invalidate-local-cache"]) {
            $this->orderCacheDb->invalidateAll(TRUE, FALSE);
            $this->orderLineCacheDb->invalidateAll(TRUE, FALSE);
        }

        if (isset($options["invalidate-remote-cache"]) && $options["invalidate-remote-cache"]) {
            $this->orderCacheDb->invalidateAll(FALSE, TRUE);
            $this->orderLineCacheDb->invalidateAll(FALSE, TRUE);
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
        //
        $this->log("updating local cache(orders)...");
        $this->counters["cache"]["index"] = 0;
        foreach (["MEKIT", "IMP"] as $database) {
            while ($localItem = $this->getNextOrder($database)) {
                if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counters["cache"]["index"] == $FORCE_LIMIT) {
                    break;
                }
                $this->counters["cache"]["index"]++;
                $this->saveOrderInCache($localItem);
            }
        }
        //
        $this->log("updating local cache(order lines)...");
        $this->counters["cache"]["index"] = 0;
        foreach (["MEKIT", "IMP"] as $database) {
            while ($localItem = $this->getNextOrderLine($database)) {
                if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counters["cache"]["index"] == $FORCE_LIMIT) {
                    break;
                }
                $this->counters["cache"]["index"]++;
                $this->saveOrderLineInCache($localItem);
            }
        }
    }


    protected function updateRemoteFromCache() {
        $FORCE_LIMIT = FALSE;
        //
        $this->log("updating remote(orders)...");
        $this->orderCacheDb->resetItemWalker();
        $this->counters["remote"]["index"] = 0;
        $registeredCodes = [];
        while ($cacheItem = $this->orderCacheDb->getNextItem('metodo_last_update_time', $orderDir = 'ASC')) {
            if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counters["remote"]["index"] == $FORCE_LIMIT) {
                break;
            }
            $this->counters["remote"]["index"]++;
            $remoteItem = $this->saveOrderOnRemote($cacheItem);
            $this->storeCrmIdForCachedOrder($cacheItem, $remoteItem);
        }
    }

    /**
     * @param \stdClass $cacheItem
     * @return \stdClass|bool
     */
    protected function saveOrderOnRemote($cacheItem) {
        $result = FALSE;
        $ISO = 'Y-m-d\TH:i:sO';
        $metodoLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->metodo_last_update_time);
        $crmLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->crm_last_update_time);

        if ($metodoLastUpdate > $crmLastUpdate) {
            $this->log(
                "-----------------------------------------------------------------------------------------"
                . $this->counters["remote"]["index"]
            );

            try {
                $crm_id = $this->loadRemoteOrderId($cacheItem);
            } catch(\Exception $e) {
                $this->log("CANNOT LOAD ID FROM CRM - UPDATE WILL BE SKIPPED: " . $e->getMessage());
                return $result;
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
                    $result = $this->sugarCrmRest->comunicate('/mkt_Orders/' . $crm_id, 'PUT', $syncItem);
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
                //CREATE
                $this->log("creating remote...");
                try {
                    $result = $this->sugarCrmRest->comunicate('/mkt_Orders', 'POST', $syncItem);
                } catch(SugarCrmRestException $e) {
                    $this->log("REMOTE INSERT ERROR!!! - " . $e->getMessage());
                }
            }
        }
        return $result;
    }

    /**
     * They would be recreated all over again
     * @param \stdClass $cacheItem
     * @return string|bool
     * @throws \Exception
     */
    protected function loadRemoteOrderId($cacheItem) {
        $crm_id = FALSE;
        $filter = [];

        if (!empty($cacheItem->crm_id)) {
            $filter[] = [
                'id' => $cacheItem->crm_id
            ];
        }
        else {
            $filter[] = [
                'database_metodo' => $cacheItem->database_metodo,
                'id_head' => $cacheItem->id_head
            ];
        }

        //try to load 2 of them - if there are more than one we do not know which one to update
        $arguments = [
            "filter" => $filter,
            "max_num" => 2,
            "offset" => 0,
            "fields" => "id",
        ];

        $result = $this->sugarCrmRest->comunicate('/mkt_Orders/filter', 'GET', $arguments);

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
    protected function storeCrmIdForCachedOrder($cacheItem, $remoteItem) {
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
            $this->orderCacheDb->updateItem($cacheUpdateItem);
        }
    }


    /**
     * @param \stdClass $localItem
     * @throws \Exception
     */
    protected function saveOrderLineInCache($localItem) {
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
        $candidates = $this->orderLineCacheDb->loadItems($filter);
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
            $updateItem = $this->generateNewOrderLineObject($localItem);
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

        //INSERT / UPDATE ORDER
        switch ($operation) {
            case "insert":
                $this->orderLineCacheDb->addItem($updateItem);
                break;
            case "update":
                $this->orderLineCacheDb->updateItem($updateItem);
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
    protected function saveOrderInCache($localItem) {
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
        $candidates = $this->orderCacheDb->loadItems($filter);
        if ($candidates && count($candidates)) {
            if (count($candidates) > 1) {
                throw new \Exception("Multiple Head Id found for db!");
            }
            $cachedItem = $candidates[0];
            $updateItem = clone($localItem);
            $updateItem->id = $cachedItem->id;
            $operation = 'update';
        }

        //not there - create new
        if (!$cachedItem) {
            $updateItem = $this->generateNewOrderObject($localItem);
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

        //INSERT / UPDATE ORDER
        switch ($operation) {
            case "insert":
                $this->orderCacheDb->addItem($updateItem);
                break;
            case "update":
                $this->orderCacheDb->updateItem($updateItem);
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
    protected function generateNewOrderObject($localItem) {
        $order = clone($localItem);
        $order->id = md5(json_encode($localItem) . microtime(TRUE));
        $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
        $order->metodo_last_update_time = $oldDate->format("c");
        $order->crm_last_update_time = $oldDate->format("c");
        return $order;
    }

    /**
     * @param \stdClass $localItem
     * @return \stdClass
     * @throws \Exception
     */
    protected function generateNewOrderLineObject($localItem) {
        $orderLine = clone($localItem);
        $orderLine->id = md5(json_encode($localItem) . microtime(TRUE));
        $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
        $orderLine->metodo_last_update_time = $oldDate->format("c");
        $orderLine->crm_last_update_time = $oldDate->format("c");
        // get order_id
        $filter = [
            'database_metodo' => $localItem->database_metodo,
            'id_head' => $localItem->id_head,
        ];
        $candidates = $this->orderCacheDb->loadItems($filter);
        if ($candidates && count($candidates)) {
            if (count($candidates) > 1) {
                throw new \Exception("Multiple Head Id found for db!");
            }
            $candidate = $candidates[0];
            $orderLine->order_id = $candidate->id;
        }
        //
        return $orderLine;
    }


    /**
     * @param string $database IMP|MEKIT
     * @return bool|\stdClass
     */
    protected function getNextOrder($database) {
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
    protected function getNextOrderLine($database) {
        if (!$this->localItemStatement2) {
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
            $this->localItemStatement2 = $db->prepare($sql);
            $this->localItemStatement2->execute();
        }
        $item = $this->localItemStatement2->fetch(\PDO::FETCH_OBJ);
        if ($item) {
            $item->database_metodo = $database;
        }
        else {
            $this->localItemStatement2 = NULL;
        }
        return $item;
    }
}