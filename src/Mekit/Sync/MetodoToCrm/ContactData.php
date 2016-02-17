<?php
/**
 * Created by Adam Jakab.
 * Date: 05/01/16
 * Time: 14.34
 */

namespace Mekit\Sync\MetodoToCrm;

use Mekit\Console\Configuration;
use Mekit\DbCache\ContactCache;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRest;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRestException;
use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;
use Monolog\Logger;

class ContactData extends Sync implements SyncInterface {
    /** @var callable */
    protected $logger;

    /** @var SugarCrmRest */
    protected $sugarCrmRest;

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
        $this->contactCacheDb = new ContactCache('Contact', $logger);
        $this->sugarCrmRest = new SugarCrmRest();
    }

    /**
     * @param array $options
     */
    public function execute($options) {
        //$this->log("EXECUTING..." . json_encode($options));
        if (isset($options["delete-cache"]) && $options["delete-cache"]) {
            $this->contactCacheDb->removeAll();
        }

        if (isset($options["invalidate-cache"]) && $options["invalidate-cache"]) {
            $this->contactCacheDb->invalidateAll(TRUE, TRUE);
        }

        if (isset($options["invalidate-local-cache"]) && $options["invalidate-local-cache"]) {
            $this->contactCacheDb->invalidateAll(TRUE, FALSE);
        }

        if (isset($options["invalidate-remote-cache"]) && $options["invalidate-remote-cache"]) {
            $this->contactCacheDb->invalidateAll(FALSE, TRUE);
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
        while ($localItem = $this->getNextLocalItem()) {
            $this->counters["cache"]["index"]++;
            $this->saveLocalItemInCache($localItem);
//            if ($this->counters["cache"]["index"] >= 1) {
//                break;
//            }
        }
    }


    protected function updateRemoteFromCache() {
        $this->log("updating remote...");
        $this->contactCacheDb->resetItemWalker();
        $this->counters["remote"]["index"] = 0;
        $registeredCodes = [];
        while ($cacheItem = $this->contactCacheDb->getNextItem()) {
            $this->counters["remote"]["index"]++;
            $remoteItem = $this->saveRemoteItem($cacheItem);
            $this->storeCrmIdForCachedItem($cacheItem, $remoteItem);
//            $RC = $this->createRelationshipsForCompanies($cacheItem);
//            $registeredCodes = array_merge($registeredCodes, $RC);
//            if ($this->counters["remote"]["index"] >= 1) {
//                break;
//            }
        }
        //this must stay outside of the loop
        //$this->updateCrmDateOnCodes($registeredCodes);
    }


    protected function updateCrmDateOnCodes($registeredCodes) {
        $this->contactCacheDb->resetItemWalker();
        $this->log("updating codes cache...");
        foreach ($registeredCodes as $registeredCode) {
            $cacheUpdateItem = new \stdClass();
            $cacheUpdateItem->id = $registeredCode;
            $now = new \DateTime();
            $cacheUpdateItem->crm_last_update_time_c = $now->format("c");
            $this->log("UPDATED CACHE CODE DATE: " . $registeredCode);
        }
    }

    /**
     * Returns list of code ids to update in cache (dates)
     *
     * @param \stdClass $cacheItem
     * @return array
     */
    protected function createRelationshipsForCompanies($cacheItem) {
        $answer = [];

        $ISO = 'Y-m-d\TH:i:sO';
        $metodoLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->metodo_last_update_time_c);
        $crmLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->crm_last_update_time_c);

        if ($metodoLastUpdate > $crmLastUpdate) {
            $this->log("CACHE: " . json_encode($cacheItem));
            $cacheContactId = $cacheItem->id;
            $crmContactId = $cacheItem->crm_id;
            $filter = [
                'contact_id' => $cacheContactId
            ];
            $contactCodes = $this->contactCodesCacheDb->loadItems($filter);
            if ($contactCodes && count($contactCodes)) {
                foreach ($contactCodes as $contactCode) {
                    $this->log("CONTACT CODE: " . json_encode($contactCode));
                    $crmAccountId = $this->loadRemoteAccountId($contactCode->database, $contactCode->metodo_code_company);
                    if ($crmAccountId) {
                        /*
                         * IMP 1-5
                         * MEKIT 6-10
                         *
                         * 1/6 = DIREZIONE
                         * 2/7 = Amministrazione
                         * 3/8 = Acquisti
                         * 4/9 = Commerciale
                         * 5/10 = Operativo + NO-ROLE
                         * */
                        $linkName = FALSE;
                        $relationshipNumber = FALSE;//accounts_contacts_[1-10]
                        switch ($contactCode->metodo_role) {
                            case "DIREZIONE":
                                $relationshipNumber = 1;
                                break;
                            case "AMMINISTRAZIONE":
                                $relationshipNumber = 2;
                                break;
                            case "ACQUISTI":
                                $relationshipNumber = 3;
                                break;
                            case "COMMERCIALE":
                                $relationshipNumber = 4;
                                break;
                            case "OPERATIVO":
                            case "NO-ROLE":
                                $relationshipNumber = 5;
                                break;
                        }
                        if ($relationshipNumber !== FALSE) {
                            $relationshipNumber += (strtoupper($contactCode->database) == "MEKIT" ? 5 : 0);
                            $linkName = 'accounts_contacts_' . $relationshipNumber;
                        }
                        if ($linkName) {
                            $this->log("REMOTE ACCOUNT ID: " . $crmAccountId);
                            $this->log("RELATIONSHIP NAME: " . $linkName);
                            try {
                                $linkData = [
                                    "link_name" => $linkName,
                                    "ids" => [$crmContactId]
                                ];
                                $result = $this->sugarCrmRest->comunicate(
                                    '/Accounts/' . $crmAccountId . '/link', 'POST', $linkData
                                );
                                $this->log("LINKED: " . json_encode($result));
                                $answer[] = $contactCode->id;
                            } catch(SugarCrmRestException $e) {
                                //go ahead with false silently
                                $this->log("REMOTE RELATIONSHIP ERROR!!! - " . $e->getMessage());
                            }
                        }
                        else {
                            $this->log(
                                "REMOTE RELATIONSHIP ERROR!!! - No link name for db:" . $contactCode->database
                                . " - role: " . $contactCode->metodo_role
                            );
                        }
                    }
                }
            }
        }
        else {
            $this->log("Relation is already up to date!");
        }


        return $answer;
    }

    /**
     * @param $database
     * @param $metodo_code
     * @return string|bool
     */
    protected function loadRemoteAccountId($database, $metodo_code) {
        $answer = FALSE;
        $type = FALSE;
        if (strtoupper(substr($metodo_code, 0, 1)) == 'C') {
            $type = 'cli';
        }
        else if (strtoupper(substr($metodo_code, 0, 1)) == 'F') {
            $type = 'sup';
        }
        $db = strtolower($database);
        if ($type && $db) {
            $filterFieldName = 'metodo_inv_' . $type . '_' . $db . '_c';

            $filter[] = [
                $filterFieldName => $metodo_code
            ];

            $arguments = [
                "filter" => $filter,
                "max_num" => 1,
                "offset" => 0,
                "fields" => "id",
            ];

            $result = $this->sugarCrmRest->comunicate('/Accounts/filter', 'GET', $arguments);

            if (isset($result) && isset($result->records) && count($result->records)) {
                $remoteItem = $result->records[0];
                $answer = $remoteItem->id;
            }
        }
        return $answer;
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
                $remoteItemIdList = $remoteItem->ids;
                $remoteItemId = $remoteItemIdList[0];
                $cacheUpdateItem->crm_id = $remoteItemId;
                $now = new \DateTime();
                $cacheUpdateItem->crm_last_update_time_c = $now->format("c");
            }
            $this->contactCacheDb->updateItem($cacheUpdateItem);
        }
    }

    /**
     * @param \stdClass $cacheItem
     * @return \stdClass|bool
     */
    protected function saveRemoteItem($cacheItem) {
        $result = FALSE;
        $ISO = 'Y-m-d\TH:i:sO';
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
                $this->log("CANNOT LOAD ID FROM CRM - UPDATE WILL BE SKIPPED: " . $e->getMessage());
                return $result;
            }


            $syncItem = clone($cacheItem);
            unset($syncItem->crm_id);
            $syncItem->metodo_contact_code_c = $syncItem->id;
            unset($syncItem->id);


            //modify sync item
            //reformat date
            $syncItem->metodo_last_update_time_c = $metodoLastUpdate->format("Y-m-d H:i:s");

            $syncItem->gender_c = 2;
            if (in_array($syncItem->salutation, [4, 8, 11, 12])) {
                $syncItem->gender_c = 1;
            }

            //add special data
            $syncItem->profiling_c = FALSE;//"Da profilare"

            //$this->log("CRM(remoteid: $crm_id) SYNC ITEM: " . json_encode($syncItem));

            //add id to sync item for update
            $restOperation = "INSERT";
            if ($crm_id) {
                $syncItem->id = $crm_id;
                $restOperation = "UPDATE";
            }

            //create arguments for rest
            $arguments = [
                'module_name' => 'Contacts',
                'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($syncItem),
            ];

            $this->log("CRM SYNC ITEM[" . $restOperation . "/" . $crm_id . "]: " . json_encode($arguments));

            try {
                $result = $this->sugarCrmRest->comunicate('set_entries', $arguments);
                $this->log("REMOTE RESULT: " . json_encode($result));
            } catch(SugarCrmRestException $e) {
                //go ahead with false silently
                $this->log("REMOTE ERROR!!! - " . $e->getMessage());
                //we must remove crm_id from $cacheItem
                //create fake result
                $result = new \stdClass();
                $result->updateFailure = TRUE;
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
    protected function loadRemoteItemId($cacheItem) {
        $crm_id = FALSE;
        $arguments = [
            'module_name' => 'Contacts',
            'query' => "contacts_cstm.metodo_contact_code_c = '" . $cacheItem->id . "'",
            'order_by' => "",
            'offset' => 0,
            'select_fields' => ['id'],
            'link_name_to_fields_array' => [],
            'max_results' => 2,
            'deleted' => FALSE,
            'Favorites' => FALSE,
        ];

        //$this->log("IDENTIFYING CRMID BY: " . json_encode($arguments));

        /** @var \stdClass $result */
        $result = $this->sugarCrmRest->comunicate('get_entry_list', $arguments);
        if (isset($result) && isset($result->entry_list)) {
            if (count($result->entry_list) > 1) {
                //This should never happen!!!
                $this->log(str_repeat("-", 120));
                $this->log(str_repeat("-", 120));
                $this->log(str_repeat("-", 120));
                $this->log(
                    "There is a multiple correspondence for requested codes!"
                    . json_encode($arguments), Logger::ERROR, $result->entry_list
                );
                $this->log("RESULTS: " . json_encode($result->entry_list));
                $this->log(str_repeat("-", 120));
                $this->log(str_repeat("-", 120));
                $this->log(str_repeat("-", 120));
                throw new \Exception(
                    "There is a multiple correspondence for requested codes!" . json_encode($arguments)
                );
            }
            if (count($result->entry_list) == 1) {
                /** @var \stdClass $remoteItem */
                $remoteItem = $result->entry_list[0];
                //$this->log("FOUND REMOTE ITEM: " . json_encode($remoteItem));
                $crm_id = $remoteItem->id;
            }
        }
        return ($crm_id);
    }


    /**
     * @param \stdClass $localItem
     * @throws \Exception
     */
    protected function saveLocalItemInCache($localItem) {
        /** @var array|bool $operations */
        $operation = FALSE;

        /** @var \stdClass $cachedItemContact */
        $cachedItemContact = FALSE;

        /** @var \stdClass $updateItemContact */
        $updateItemContact = FALSE;

        /** @var array $warnings */
        $warnings = [];

        $this->log(
            "-----------------------------------------------------------------------------------------"
            . $this->counters["cache"]["index"]
        );

        //get contact from cache by
        $filter = [
            'id' => $localItem->id,
        ];
        $codeCandidates = $this->contactCacheDb->loadItems($filter);

        if ($codeCandidates && count($codeCandidates)) {
            if (count($codeCandidates) > 1) {
                throw new \Exception("Multiple Codes for '" . $localItem->id . "'!");
            }
            $cachedItemContact = $codeCandidates[0];
            $updateItemContact = clone($cachedItemContact);
            $operation = 'update';
        }
        else {
            $updateItemContact = clone($localItem);
            $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
            $updateItemContact->crm_last_update_time_c = $oldDate->format("c");
            $operation = 'insert';
        }


        //add other data on item only if localData is newer that cached data
        $metodoLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->metodo_last_update_time_c);
        $updateItemLastUpdateTime = new \DateTime($updateItemContact->metodo_last_update_time_c);
        if ($metodoLastUpdateTime > $updateItemLastUpdateTime) {
            foreach (get_object_vars($updateItemContact) as $k => $v) {
                if (isset($localItem->$k)) {
                    $updateItemContact->$k = $localItem->$k;
                }
            }
        }

        //DECIDE OPERATION - CODES
        $operation = ($cachedItemContact == $updateItemContact) ? "skip" : $operation;

        //LOG
        if ($operation != "skip") {
            $this->log("[" . $operation . "]:");
            $this->log("LOCAL: " . json_encode($localItem));
            //$this->log("CODES: " . json_encode($cachedItemContactCodes));
            //$this->log("CONTACT(C): " . json_encode($cachedItemContact));
            $this->log("CONTACT(U): " . json_encode($updateItemContact));

        }
        if (!empty($warnings)) {
            foreach ($warnings as $warning) {
                $this->log("WARNING: " . $warning);
            }
        }

        //INSERT / UPDATE CONTACT
        switch ($operation) {
            case "insert":
                $this->contactCacheDb->addItem($updateItemContact);
                break;
            case "update":
                $this->contactCacheDb->updateItem($updateItemContact);
                break;
            case "skip":
                break;
            default:
                throw new \Exception("Code operation(" . $operations["contact"] . ") is not implemented!");
        }
    }


    /**
     * @param \stdClass $localItem
     * @return \stdClass
     */
    protected function generateNewContactObject($localItem) {
        $contact = new \stdClass();
        $contact->id = md5(json_encode($localItem) . microtime(TRUE));
        $contact->email = '[]';
        $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
        $contact->metodo_last_update_time_c = $oldDate->format("c");
        $contact->crm_last_update_time_c = $oldDate->format("c");
        return $contact;
    }

    /**
     * @param \stdClass $localItem
     * @return \stdClass
     */
    protected function generateNewContactCodeObject($localItem) {
        $code = new \stdClass();
        $code->id = md5(json_encode($localItem) . microtime(TRUE));
        $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
        $code->metodo_last_update_time_c = $oldDate->format("c");
        $code->crm_last_update_time_c = $oldDate->format("c");
        return $code;
    }




    /**
     * @return bool|\stdClass
     */
    protected function getNextLocalItem() {
        if (!$this->localItemStatement) {
            $db = Configuration::getDatabaseConnection("SERVER2K8");
            $sql = "SELECT
                AN.IdContatto AS id,
                AN.Cognome AS last_name,
                AN.Nome AS first_name,
                AN.Titolo AS salutation,
                AN.Cellulare AS phone_mobile,
                AN.TelUfficio AS phone_work,
                AN.TelCasa AS phone_home,
                AN.Fax AS phone_fax,
                AN.Email AS email1,
                AN.Note AS description,
                AN.DataModifica AS metodo_last_update_time_c
                FROM [Crm2Metodo].[dbo].[AnagraficaContatti] AS AN
                ";
            $this->localItemStatement = $db->prepare($sql);
            $this->localItemStatement->execute();
        }
        $item = $this->localItemStatement->fetch(\PDO::FETCH_OBJ);
        if ($item) {
            $item->first_name = strtoupper(trim($item->first_name));
            $item->last_name = strtoupper(trim($item->last_name));
            $item->email1 = strtolower(trim($item->email1));
            $item->phone_mobile = $this->normalizePhoneNumber($item->phone_mobile);
            $item->phone_work = $this->normalizePhoneNumber($item->phone_work);
            $item->phone_home = $this->normalizePhoneNumber($item->phone_home);
            $item->phone_fax = $this->normalizePhoneNumber($item->phone_fax);

            $metodoLastUpdate = \DateTime::createFromFormat('Y-m-d H:i:s.u', $item->metodo_last_update_time_c);
            $item->metodo_last_update_time_c = $metodoLastUpdate->format("c");
        }
        else {
            $this->localItemStatement = NULL;
        }
        return $item;
    }


    /**
     * @param string $dataString
     * @param string $db
     * @param string $key
     * @param string $value
     * @return string
     */
    protected function addElementToJsonData($dataString, $db, $key, $value) {
        $answer = $dataString;
        if (!empty($value)) {
            $data = json_decode($dataString);
            if ($db) {
                if (!isset($data->$db)) {
                    $data->$db = [];
                }
                $dbData = (array) $data->$db;
            }
            else {
                $dbData = $data;
            }
            if ($key) {
                if (!array_key_exists($key, $dbData)) {
                    $dbData[$key] = $value;
                }
            }
            else {
                if (!in_array($value, $dbData)) {
                    $dbData[] = $value;
                }
            }
            if ($db) {
                $data->$db = $dbData;
            }
            else {
                $data = $dbData;
            }
            $answer = json_encode($data);
        }
        return $answer;
    }

    /**
     * @param string $phoneNumber
     * @return string mixed
     */
    protected function normalizePhoneNumber($phoneNumber) {
        $answer = trim((string) $phoneNumber);
        $answer = preg_replace('#[^0-9]#', '', $answer);
        return $answer;
    }

    /**
     * UNUSED !?
     * @param int $code
     * @return string
     */
    protected function getSalutationFromCode($code) {
        $salutations = [
            1 => "Avv.",
            2 => "Cav.",
            3 => "Dott.",
            4 => "Dott.ssa",
            5 => "Geom.",
            6 => "Ing.",
            7 => "Prof.",
            8 => "Prof.ssa",
            9 => "Rag.",
            10 => "Sig.",
            11 => "Sig.na",
            12 => "Sig.ra",
            13 => "Sigg."
        ];
        $salutation = array_key_exists($code, $salutations) ? $salutations[$code] : "";
        return $salutation;
    }
}