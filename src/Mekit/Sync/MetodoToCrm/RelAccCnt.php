<?php
/**
 * Created by Adam Jakab.
 * Date: 17/02/16
 * Time: 16.10
 */

namespace Mekit\Sync\MetodoToCrm;

use Mekit\Console\Configuration;
use Mekit\DbCache\RelAccCntCache;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRest;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRestException;
use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;
use Monolog\Logger;


class RelAccCnt extends Sync implements SyncInterface {
    /** @var callable */
    protected $logger;

    /** @var SugarCrmRest */
    protected $sugarCrmRest;

    /** @var  RelAccCntCache */
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
        $this->cacheDb = new RelAccCntCache('RelAccCnt', $logger);
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
            //$this->updateRemoteFromCache();
        }
    }

    /**
     * get data from Metodo and store it in local cache
     */
    protected function updateLocalCache() {
        $this->log("updating local cache...");
        $this->counters["cache"]["index"] = 0;
        foreach (["MEKIT", "IMP"] as $database) {
            while ($localItem = $this->getNextLocalItem($database)) {
                $this->counters["cache"]["index"]++;
                $this->saveLocalItemInCache($localItem);
//                if ($this->counters["cache"]["index"] >= 10) {
//                    $this->cacheDb->resetItemWalker();
//                    break;
//                }
            }
        }
    }

    /**
     * @param \stdClass $localItem
     * @throws \Exception
     */
    protected function saveLocalItemInCache($localItem) {
        /** @var array|bool $operations */
        $operation = FALSE;

        /** @var \stdClass $cachedItem */
        $cachedItem = FALSE;

        /** @var \stdClass $updateItem */
        $updateItem = FALSE;

        /** @var array $warnings */
        $warnings = [];

        $this->log(
            "-----------------------------------------------------------------------------------------"
            . $this->counters["cache"]["index"]
        );

        //get contact from cache by
        $filter = [
            'metodo_contact_id' => $localItem->metodo_contact_id,
            'metodo_cf_id' => $localItem->metodo_cf_id,
            'metodo_role_id' => $localItem->metodo_role_id,
        ];
        $candidates = $this->cacheDb->loadItems($filter);

        if ($candidates && count($candidates)) {
            if (count($candidates) > 1) {
                throw new \Exception("Multiple Cache Items for '" . json_encode($filter) . "'!");
            }
            //$cachedItem = $candidates[0];
            //$updateItem = clone($cachedItem);
            //$operation = 'update';
            $operation = 'skip';
        }
        else {
            $updateItem = clone($localItem);
            $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
            $updateItem->crm_last_update_time_c = $oldDate->format("c");
            $updateItem->id = md5(json_encode($localItem) . "-" . microtime(TRUE));
            $operation = 'insert';
        }

        if ($operation != 'skip') {
            //add other data on item only if localData is newer that cached data
            $metodoLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->metodo_last_update_time_c);
            $updateItemLastUpdateTime = new \DateTime($updateItem->metodo_last_update_time_c);
            if ($metodoLastUpdateTime > $updateItemLastUpdateTime) {
                foreach (get_object_vars($updateItem) as $k => $v) {
                    if (isset($localItem->$k)) {
                        $updateItem->$k = $localItem->$k;
                    }
                }
            }
        }


        //DECIDE OPERATION - CODES
        $operation = ($cachedItem == $updateItem) ? "skip" : $operation;

        //LOG
        if ($operation != "skip") {
            $this->log("[" . $operation . "]:");
            $this->log("LOCAL: " . json_encode($localItem));
            $this->log("CONTACT(C): " . json_encode($cachedItem));
            $this->log("CONTACT(U): " . json_encode($updateItem));

        }
        if (!empty($warnings)) {
            foreach ($warnings as $warning) {
                $this->log("WARNING: " . $warning);
            }
        }

        //INSERT / UPDATE CONTACT
        switch ($operation) {
            case "insert":
                $this->cacheDb->addItem($updateItem);
                break;
            case "update":
                $this->cacheDb->updateItem($updateItem);
                break;
            case "skip":
                break;
            default:
                throw new \Exception("Code operation(" . $operations["contact"] . ") is not implemented!");
        }
    }


    /**
     * @param string $database
     * @return bool|\stdClass
     */
    protected function getNextLocalItem($database) {
        if (!$this->localItemStatement) {
            $db = Configuration::getDatabaseConnection("SERVER2K8");
            $sql = "SELECT
                TP.IdContatto AS metodo_contact_id,
                TP.RIFCODCONTO AS metodo_cf_id,
                TP.CodRuolo AS metodo_role_id,
                TP.DATAMODIFICA AS metodo_last_update_time_c
                FROM [$database].[dbo].[TABELLAPERSONALE] AS TP
                ";
            $this->localItemStatement = $db->prepare($sql);
            $this->localItemStatement->execute();
        }
        $item = $this->localItemStatement->fetch(\PDO::FETCH_OBJ);
        if ($item) {
            try {
                $item->rel_table = $this->getRelationshipTableName($database, $item->metodo_role_id);
            } catch(\Exception $e) {
                $item->rel_table = '';
                $this->log(
                    "WARNING! - Contact without a role(db: $database) in " . $item->metodo_cf_id, Logger::CRITICAL
                );
            }
            $metodoLastUpdate = \DateTime::createFromFormat('Y-m-d H:i:s.u', $item->metodo_last_update_time_c);
            $item->metodo_last_update_time_c = $metodoLastUpdate->format("c");
        }
        else {
            $this->localItemStatement = NULL;
        }
        return $item;
    }


    /**
     * @param string $database
     * @param string $roleId
     * @return string
     * @throws \Exception
     */
    protected function getRelationshipTableName($database, $roleId) {
        switch ($database) {
            case "IMP":
                switch ($roleId) {
                    case "1":
                        $answer = "accounts_contacts_imp_acq";
                        break;
                    case "2":
                        $answer = "accounts_contacts_imp_dir";
                        break;
                    case "3":
                        $answer = "accounts_contacts_imp_com";
                        break;
                    case "4":
                    case "5":
                        $answer = "accounts_contacts_imp_opr";
                        break;
                    case "6":
                        $answer = "accounts_contacts_imp_adm";
                        break;
                    default:
                        throw new \Exception("Unable to identify relationship table($database - $roleId)!");
                }
                break;
            case "MEKIT":
                switch ($roleId) {
                    case "1":
                        $answer = "accounts_contacts_mekit_acq";
                        break;
                    case "2":
                        $answer = "accounts_contacts_mekit_dir";
                        break;
                    case "3":
                        $answer = "accounts_contacts_mekit_com";
                        break;
                    case "4":
                    case "5":
                        $answer = "accounts_contacts_mekit_opr";
                        break;
                    case "6":
                        $answer = "accounts_contacts_mekit_adm";
                        break;
                    default:
                        throw new \Exception("Unable to identify relationship table($database - $roleId)!");
                }
                break;
            default:
                throw new \Exception("Unable to identify relationship table($database - $roleId)!");
        }
        return $answer;
    }

}