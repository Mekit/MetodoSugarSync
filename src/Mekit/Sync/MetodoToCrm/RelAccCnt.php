<?php
/**
 * Created by Adam Jakab.
 * Date: 17/02/16
 * Time: 16.10
 */

namespace Mekit\Sync\MetodoToCrm;

use Mekit\Console\Configuration;
use Mekit\DbCache\AccountCache;
use Mekit\DbCache\ContactCache;
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

    /** @var  AccountCache */
    protected $accountCacheDb;

    /** @var  ContactCache */
    protected $contactCacheDb;

    /** @var  \PDOStatement */
    protected $localItemStatement;

    /** @var array */
    protected $counters = [];

    /**
     * @param callable $logger
     */
    public function __construct($logger) {
        parent::__construct($logger);
        $this->accountCacheDb = new AccountCache('Account', $logger);
        $this->contactCacheDb = new ContactCache('Contact', $logger);
        $this->cacheDb = new RelAccCntCache('RelAccCnt', $logger);
        $this->sugarCrmRest = new SugarCrmRest();
    }

    /**
     * @param array $options
     * @param array $arguments
     */
  public function execute($options, $arguments)
  {
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

    protected function updateRemoteFromCache() {
        $this->log("updating remote...");
        $this->cacheDb->resetItemWalker();
        $this->counters["remote"]["index"] = 0;
        while ($cacheItem = $this->cacheDb->getNextItem()) {
            $this->counters["remote"]["index"]++;
            $relationshipResult = $this->saveRemoteRelationship($cacheItem);
            $this->storeCrmIdForCachedItem($cacheItem, $relationshipResult);
//            if ($this->counters["remote"]["index"] >= 50) {
//                break;
//            }
        }
    }

    /**
     * @param \stdClass $cacheItem
     * @param array     $relationshipResult
     */
    protected function storeCrmIdForCachedItem($cacheItem, $relationshipResult) {
        if ($relationshipResult) {
            $cacheUpdateItem = new \stdClass();
            $cacheUpdateItem->id = $cacheItem->id;
            if (!$relationshipResult['has_relationship']) {
                //we must remove crm_id and reset crm_last_update_time_c on $cacheItem
                $cacheUpdateItem->crm_account_id = NULL;
                $cacheUpdateItem->crm_contact_id = NULL;
                $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
                $cacheUpdateItem->crm_last_update_time_c = $oldDate->format("c");
            }
            else {
                $cacheUpdateItem->crm_account_id = $relationshipResult['account_id'];
                $cacheUpdateItem->crm_contact_id = $relationshipResult['contact_id'];
                $now = new \DateTime();
                $cacheUpdateItem->crm_last_update_time_c = $now->format("c");
            }

            $this->log("UPDATING cache item: " . json_encode($cacheUpdateItem));
            $this->cacheDb->updateItem($cacheUpdateItem);

        }
    }

    /**
     * @param \stdClass $cacheItem
     * @return \array|bool
     */
    protected function saveRemoteRelationship($cacheItem) {
        $result = FALSE;
        $ISO = 'Y-m-d\TH:i:sO';
        $metodoLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->metodo_last_update_time_c);
        $crmLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->crm_last_update_time_c);

        if (!$cacheItem->rel_table) {
            $this->log("Warning! No Relationship table for: " . json_encode($cacheItem));
            return FALSE;
        }

        if ($metodoLastUpdate > $crmLastUpdate) {
            $this->log(
                "-----------------------------------------------------------------------------------------"
                . $this->counters["remote"]["index"]
            );


            //$this->log(json_encode($cacheItem));

            try {
                $relationship = $this->loadRemoteRelationship($cacheItem);
            } catch(\Exception $e) {
                $this->log("CANNOT LOAD RELATIONSHIP - UPDATE WILL BE SKIPPED: " . $e->getMessage());
                return $result;
            }


            if ($relationship['has_relationship'] === TRUE) {
                $result = $relationship;
                $this->log("Relationship already exists - SKIPPING");
            }

            if ($result == FALSE) {
                $this->log("CREATING RELATIONSHIP FOR: " . json_encode($relationship));
                $arguments = array(
                    'module_name' => 'Accounts',
                    'module_id' => $relationship['account_id'],
                    'link_field_name' => $relationship['relationship_name'],
                    'related_ids' => [$relationship['contact_id']],
                    'name_value_list' => []
                );
                $relResult = $this->sugarCrmRest->comunicate('set_relationship', $arguments);
                if ($relResult && isset($relResult->created) && $relResult->created == 1) {
                    $result = $relationship;
                    $result['has_relationship'] = TRUE;
                }
                else {
                    $this->log("FAILED RELATIONSHIP: " . json_encode($relResult));
                }
            }
        }
        return $result;
    }

    /**
     * They would be recreated all over again
     * @param \stdClass $cacheItem
     * @return array
     * @throws \Exception
     */
    protected function loadRemoteRelationship($cacheItem) {
        $answer = [
            'account_id' => FALSE,
            'contact_id' => FALSE,
            'relationship_name' => FALSE,
            'has_relationship' => FALSE
        ];

        //FIND ACCOUNT CRM ID FROM AccountCache
        $filterFieldName = $this->getAccountCacheFieldNameForCodiceMetodo($cacheItem->database, $cacheItem->metodo_cf_id);
        $filter = [
            $filterFieldName => $cacheItem->metodo_cf_id,
        ];
        $accounts = $this->accountCacheDb->loadItems($filter);
        if (count($accounts) > 1) {
            throw new \Exception("Multiple cached Accounts found for: " . $cacheItem->metodo_cf_id);
        }
        if (!count($accounts)) {
            throw new \Exception("No cached Accounts found for: " . $cacheItem->metodo_cf_id);
        }
        $account = $accounts[0];
        if (!isset($account->crm_id) || empty($account->crm_id)) {
            throw new \Exception("Cached Account does not have CRMID for: " . $cacheItem->metodo_cf_id);
        }
        $accountCrmId = $account->crm_id;
        $answer['account_id'] = $accountCrmId;
        //$this->log("CACHED ACCOUNT CRMID: " . $accountCrmId);

        //FIND CONTACT CRM ID FROM ContactCache
        $filter = [
            'id' => $cacheItem->metodo_contact_id,
        ];
        $contacts = $this->contactCacheDb->loadItems($filter);
        if (count($contacts) > 1) {
            throw new \Exception("Multiple cached Contacts found for: " . $cacheItem->metodo_contact_id);
        }
        if (!count($contacts)) {
            throw new \Exception("No cached Contacts found for: " . json_encode($cacheItem));
        }
        $contact = $contacts[0];
        if (!isset($contact->crm_id) || empty($contact->crm_id)) {
            throw new \Exception("Cached Contact does not have CRMID for: " . $cacheItem->metodo_contact_id);
        }
        $contactCrmId = $contact->crm_id;
        $answer['contact_id'] = $contactCrmId;
        //$this->log("CACHED CONTACT CRMID: " . $contactCrmId);


        //FIND ACCOUNT - CONTACT RELATIONSHIP
        $answer['relationship_name'] = $cacheItem->rel_table;
        $arguments = [
            'module_name' => 'Accounts',
            'query' => "accounts.id = '" . $accountCrmId . "'",
            'order_by' => "",
            'offset' => 0,
            'select_fields' => ['id', 'name'],
            'link_name_to_fields_array' => [
                [
                    'name' => $cacheItem->rel_table,
                    'value' => ['id']
                ]
            ],
            'max_results' => 2,
            'deleted' => FALSE,
            'Favorites' => FALSE,
        ];

        /** @var \stdClass $result */
        $relResult = $this->sugarCrmRest->comunicate('get_entry_list', $arguments);
        if ($relResult) {
            if (isset($relResult->relationship_list)) {
                if (count($relResult->relationship_list) > 1) {
                    throw new \Exception("Multiple remote Relationships found for: " . json_encode($arguments));
                }
                if (count($relResult->relationship_list)) {
                    $relList = $relResult->relationship_list[0];
                    if (isset($relList->link_list)) {
                        $relLinkList = $relList->link_list;
                        foreach ($relLinkList as $relLink) {
                            if ($relLink->name == $cacheItem->rel_table) {
                                if (isset($relLink->records[0])) {
                                    $relLink = $relLink->records[0];
                                    if (isset($relLink->link_value)) {
                                        $linkValue = $relLink->link_value;
                                        if (isset($linkValue->id->value)) {
                                            $linkValId = $linkValue->id->value;
                                            if ($linkValId == $contactCrmId) {
                                                $answer['has_relationship'] = TRUE;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        //$this->log("REMOTE REL: " . json_encode($relResult));
        return $answer;
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

        //get contact from cache by
        $filter = [
            'metodo_contact_id' => $localItem->metodo_contact_id,
            'metodo_cf_id' => $localItem->metodo_cf_id,
            'metodo_role_id' => $localItem->metodo_role_id,
            'database' => $localItem->database,
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
            $this->log(
                "-----------------------------------------------------------------------------------------"
                . $this->counters["cache"]["index"]
            );
            //$this->log("[" . $operation . "]:");
            //$this->log("LOCAL: " . json_encode($localItem));
            //$this->log("CONTACT(C): " . json_encode($cachedItem));
            //$this->log("CONTACT(U): " . json_encode($updateItem));

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
            $item->database = $database;
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
     * @param string $code
     * @return string
     * @throws \Exception
     */
    protected function getAccountCacheFieldNameForCodiceMetodo($database, $code) {
        $type = strtoupper(substr($code, 0, 1));
        switch ($database) {
            case "IMP":
                switch ($type) {
                    case "C":
                        $answer = "imp_metodo_client_code_c";       //imp_metodo_client_code_c
                        break;
                    case "F":
                        $answer = "imp_metodo_supplier_code_c";     //imp_metodo_supplier_code_c
                        break;
                    default:
                        throw new \Exception("Local item needs to have Tipologia C|F!");
                }
                break;
            case "MEKIT":
                switch ($type) {
                    case "C":
                        $answer = "mekit_metodo_client_code_c";     //mekit_metodo_client_code_c
                        break;
                    case "F":
                        $answer = "mekit_metodo_supplier_code_c";   //mekit_metodo_supplier_code_c
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