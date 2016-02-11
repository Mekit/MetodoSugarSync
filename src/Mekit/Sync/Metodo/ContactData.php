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
use Mekit\SugarCrm\Rest\v10\SugarCrmRest;
use Mekit\SugarCrm\Rest\v10\SugarCrmRestException;
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

    /** @var  ContactCodesCache */
    protected $contactCodesCacheDb;

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
        $this->contactCodesCacheDb = new ContactCodesCache('Contact_Codes', $logger);
        $this->sugarCrmRest = new SugarCrmRest();
    }

    /**
     * @param array $options
     */
    public function execute($options) {
        //$this->log("EXECUTING..." . json_encode($options));
        if (isset($options["delete-cache"]) && $options["delete-cache"]) {
            $this->contactCacheDb->removeAll();
            $this->contactCodesCacheDb->removeAll();
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
            $this->cleanupOrphansFromLocalCache();
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
                /*
                if ($this->counters["cache"]["index"] == 100) {
                    break;
                }*/
                $this->counters["cache"]["index"]++;
                $this->saveLocalItemInCache($localItem);
            }
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
            $RC = $this->createRelationshipsForCompanies($cacheItem);
            $registeredCodes = array_merge($registeredCodes, $RC);
//            if ($this->counters["remote"]["index"] >= 50) {
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
            $this->contactCodesCacheDb->updateItem($cacheUpdateItem);
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
                $cacheUpdateItem->crm_id = $remoteItem->id;
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

            //modify sync item
            if (!empty($syncItem->email)) {
                $emailArray = json_decode($syncItem->email);
                if (is_array($emailArray) && count($emailArray)) {
                    $syncItem->email = [];
                    $primary = TRUE;
                    foreach ($emailArray as $email) {
                        $syncItem->email[] = [
                            "email_address" => $email,
                            "invalid_email" => FALSE,
                            "opt_out" => FALSE,
                            "primary_address" => $primary,
                            "reply_to_address" => FALSE
                        ];
                        $primary = FALSE;
                    }
                }
            }
            if (!empty($syncItem->salutation)) {
                $syncItem->gender_c = "M";
                if (in_array($syncItem->salutation, ['Sig.ra', 'Sig.na', 'Dott.ssa', 'Prof.ssa'])) {
                    $syncItem->gender_c = "F";
                }
            }

            //add special data
            $syncItem->profiling_c = FALSE;//"Da profilare"

            //unset data
            unset($syncItem->crm_id);
            unset($syncItem->id);


            $this->log("CRM SYNC ITEM: " . json_encode($syncItem));

            if ($crm_id) {
                //UPDATE
                $this->log("updating remote($crm_id): " . $syncItem->first_name . " " . $syncItem->last_name);
                try {
                    $result = $this->sugarCrmRest->comunicate('/Contacts/' . $crm_id, 'PUT', $syncItem);
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
                $this->log("creating remote: " . $syncItem->first_name . " " . $syncItem->last_name);
                try {
                    $result = $this->sugarCrmRest->comunicate('/Contacts', 'POST', $syncItem);
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
    protected function loadRemoteItemId($cacheItem) {
        $crm_id = FALSE;
        $filter = [];

        if (!empty($cacheItem->crm_id)) {
            $filter[] = [
                'id' => $cacheItem->crm_id
            ];
        }
        else {
            if (!empty($cacheItem->phone_mobile)) {
                $filter[] = [
                    'phone_mobile' => $cacheItem->phone_mobile
                ];
            }
            else if (!empty($cacheItem->first_name) && !empty($cacheItem->last_name)) {
                $filter[] = [
                    'first_name' => $cacheItem->first_name,
                    'last_name' => $cacheItem->last_name
                ];
            }
        }

        //try to load 2 of them - if there are more than one we do not know which one to update
        if (count($filter)) {
            $arguments = [
                "filter" => $filter,
                "max_num" => 2,
                "offset" => 0,
                "fields" => "id",
            ];

            $result = $this->sugarCrmRest->comunicate('/Contacts/filter', 'GET', $arguments);

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
        }
        else {
            //This happens when filter is empty
            throw new \Exception("CacheItem does not have usable data for filter!");
        }
        return ($crm_id);
    }


    /**
     * check and remove:
     * 1) contacts without codes:
     * SELECT C.id AS ORPHAN FROM Contact AS C LEFT OUTER JOIN Contact_Codes AS CC ON C.id = CC.contact_id WHERE CC.id IS NULL;
     *
     * 2) codes without corresponding contact:
     * SELECT CC.id AS ORPHAN FROM Contact_Codes AS CC LEFT OUTER JOIN Contact AS C ON CC.contact_id = C.id WHERE C.id IS NULL;
     *
     */
    protected function cleanupOrphansFromLocalCache() {
        $db = $this->contactCacheDb->getDb();
        $tableNameContact = 'Contact';
        $tableNameContactCodes = 'Contact_Codes';

        //CONTACT ORPHANS
        $query = "SELECT C.id FROM $tableNameContact AS C LEFT OUTER JOIN $tableNameContactCodes AS CC ON C.id = CC.contact_id WHERE CC.id IS NULL;";
        $statement = $db->prepare($query);
        if ($statement->execute()) {
            $orphans = $statement->fetchAll(\PDO::FETCH_COLUMN);
            if (count($orphans)) {
                $orphans = array_map(
                    function ($el) {
                        return "'" . $el . "'";
                    }, $orphans
                );
                $this->log("DELETING ORPHANED CONTACTS: " . count($orphans));
                $query = "DELETE FROM $tableNameContact WHERE id IN (" . implode(",", $orphans) . ")";
                $statement = $db->prepare($query);
                $statement->execute();
            }
        }

        //CODE ORPHANS
        $query = "SELECT CC.id FROM $tableNameContactCodes AS CC LEFT OUTER JOIN $tableNameContact AS C ON CC.contact_id = C.id WHERE C.id IS NULL;";
        $statement = $db->prepare($query);
        if ($statement->execute()) {
            $orphans = $statement->fetchAll(\PDO::FETCH_COLUMN);
            if (count($orphans)) {
                $this->log("ORPHANED CODES: " . count($orphans));
                $orphans = array_map(
                    function ($el) {
                        return "'" . $el . "'";
                    }, $orphans
                );
                $this->log("DELETING ORPHANED CODES: " . count($orphans));
                $query = "DELETE FROM $tableNameContactCodes WHERE id IN (" . implode(",", $orphans) . ")";
                $statement = $db->prepare($query);
                $statement->execute();
            }
        }
    }

    /**
     * @param \stdClass $localItem
     * @throws \Exception
     */
    protected function saveLocalItemInCache($localItem) {
        /** @var array|bool $operations */
        $operations = FALSE;

        /** @var \stdClass $cachedItemContact */
        $cachedItemContact = FALSE;

        /** @var \stdClass $updateItemContact */
        $updateItemContact = FALSE;

        /** @var \stdClass $cachedItemContactCodes */
        $cachedItemContactCodes = FALSE;

        /** @var \stdClass $updateItemContactCodes */
        $updateItemContactCodes = FALSE;

        /** @var string $identifiedBy */
        $identifiedBy = FALSE;

        /** @var array $warnings */
        $warnings = [];

        /** @var string $itemDb IMP|MEKIT */
        $itemDb = $localItem->database;

        $this->log(
            "-----------------------------------------------------------------------------------------"
            . $this->counters["cache"]["index"]
        );


        //get contact code
        $filter = [
            'database' => $itemDb,
            'metodo_contact_code' => $localItem->metodoCombinedCode,
        ];
        $codeCandidates = $this->contactCodesCacheDb->loadItems($filter);
        if ($codeCandidates && count($codeCandidates)) {
            if (count($codeCandidates) > 1) {
                throw new \Exception("Multiple Codes for '" . $localItem->metodoCombinedCode . "' for db:$itemDb!");
            }
            $cachedItemContactCodes = $codeCandidates[0];
            $updateItemContactCodes = clone($cachedItemContactCodes);
            $operations["code"] = 'update';
            //update role
            $updateItemContactCodes->metodo_role = $localItem->role;
            //
            //if we have code get contact cache data right away
            $contactId = $updateItemContactCodes->contact_id;
            $filter = [
                'id' => $contactId,
            ];
            $contactCandidates = $this->contactCacheDb->loadItems($filter);
            if ($contactCandidates && count($contactCandidates)) {
                $cachedItemContact = $contactCandidates[0];
                $operations["contact"] = 'update';
                $updateItemContact = clone($cachedItemContact);
            }
        }
        else {
            //no code was found
            $updateItemContactCodes = $this->generateNewContactCodeObject($localItem);
            //$updateItemContactCodes->contact_id = $contactId; // - this will be assigned later
            $updateItemContactCodes->database = $itemDb;
            $updateItemContactCodes->metodo_contact_code = $localItem->metodoCombinedCode;
            $updateItemContactCodes->metodo_code_company = $localItem->codiceMetodoAzienda;
            $updateItemContactCodes->metodo_code_cf = $localItem->clienteDiFatturazione;
            $updateItemContactCodes->metodo_role = $localItem->role;
            $operations["code"] = 'insert';
        }

        //search by: mobile phone
        if (!$cachedItemContact) {
            if (!empty($localItem->phone_mobile)) {
                $filter = [
                    'phone_mobile' => $localItem->phone_mobile
                ];
                $candidates = $this->contactCacheDb->loadItems($filter);
                if ($candidates) {

                    //control if we have a corresponding ClienteDiFatturazione for any of the candidates
                    foreach ($candidates as $candidate) {
                        $filter = [
                            'contact_id' => $candidate->id,
                            'database' => $itemDb,
                            'metodo_code_cf' => $localItem->clienteDiFatturazione
                        ];
                        $codeCandidates = $this->contactCodesCacheDb->loadItems($filter);
                        if ($codeCandidates && count($codeCandidates)) {
                            $cachedItemContact = $candidate;
                            $operations["contact"] = 'update';
                            $updateItemContact = clone($cachedItemContact);
                            $updateItemContactCodes->contact_id = $updateItemContact->id;
                            break;
                        }
                    }

                    //control if any of the candidates is identifiable by email
                    foreach ($candidates as $candidate) {
                        $emails = json_decode($candidate->email);
                        if (in_array($localItem->email, $emails)) {
                            $cachedItemContact = $candidate;
                            $operations["contact"] = 'update';
                            $updateItemContact = clone($cachedItemContact);
                            $updateItemContactCodes->contact_id = $updateItemContact->id;
                            break;
                        }
                    }

                    /*
                    $this->log("MOBILE CANDIDATES FOUND! " . json_encode($candidates));
                    $this->log("LOCAL: " . json_encode($localItem));
                    $this->log("CODES: " . json_encode($cachedItemContactCodes));
                    $this->log("OPERATIONS: " . json_encode($operations));
                    die("K");
                    */
                }

                //contact has an unregistered mobile
                if (!$cachedItemContact) {
                    $cachedItemContact = $this->generateNewContactObject($localItem);
                    $operations["contact"] = 'insert';
                    $updateItemContact = clone($cachedItemContact);
                    $updateItemContactCodes->contact_id = $updateItemContact->id;
                }
            }
        }

        //search by: first_name or last_name or both
        if (!$cachedItemContact) {

            $filter = [];
            if (!empty($localItem->first_name)) {
                $filter["first_name"] = $localItem->first_name;
            }
            if (!empty($localItem->last_name)) {
                $filter["last_name"] = $localItem->last_name;
            }
            $candidates = $this->contactCacheDb->loadItems($filter);
            if ($candidates) {

                //control if we have a corresponding ClienteDiFatturazione for any of the candidates
                foreach ($candidates as $candidate) {
                    $filter = [
                        'contact_id' => $candidate->id,
                        'database' => $itemDb,
                        'metodo_code_cf' => $localItem->clienteDiFatturazione
                    ];
                    $codeCandidates = $this->contactCodesCacheDb->loadItems($filter);
                    if ($codeCandidates && count($codeCandidates)) {
                        $cachedItemContact = $candidate;
                        $operations["contact"] = 'update';
                        $updateItemContact = clone($cachedItemContact);
                        $updateItemContactCodes->contact_id = $updateItemContact->id;
                        break;
                    }
                }
                /*
                $this->log("NAME CANDIDATES FOUND! " . json_encode($candidates));
                $this->log("LOCAL: " . json_encode($localItem));
                $this->log("CODES: " . json_encode($cachedItemContactCodes));
                $this->log("OPERATIONS: " . json_encode($operations));
                die("K");
                */
            }
        }

        //search by: email
        if (!$cachedItemContact) {
            $filter = [];
            if (!empty($localItem->email)) {
                $filter["email"] = $localItem->email;
            }
            $candidates = $this->contactCacheDb->loadItems($filter);
            if ($candidates) {
                /*
                if (count($candidates) > 1) {
                    throw new \Exception("Search by Email returned multiple results!");
                }*/
                $this->log("*** IDENTIFIED BY MAIL(" . $localItem->email . ") ***");
                $cachedItemContact = $candidates[0];
                $operations["contact"] = 'update';
                $updateItemContact = clone($cachedItemContact);
                $updateItemContactCodes->contact_id = $updateItemContact->id;
            }
        }


        //all search failed - create new contact
        if (!$cachedItemContact) {
            $updateItemContact = $this->generateNewContactObject($localItem);
            $operations["contact"] = 'insert';
            $updateItemContactCodes->contact_id = $updateItemContact->id;
        }

        //update contact data

        //mobile
        if (!empty($localItem->phone_mobile)) {
            $updateItemContact->phone_mobile = $localItem->phone_mobile;
        }

        //email
        $updateItemContact->email =
            $this->addElementToJsonData($updateItemContact->email, NULL, NULL, $localItem->email);

        //add other data on item only if localData is newer that cached data
        $metodoLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->DataDiModifica);
        $updateItemLastUpdateTime = new \DateTime($updateItemContact->metodo_last_update_time_c);

        if ($metodoLastUpdateTime > $updateItemLastUpdateTime) {

            $updateItemContact->metodo_last_update_time_c = $metodoLastUpdateTime->format("c");
            $updateItemContactCodes->metodo_last_update_time_c = $metodoLastUpdateTime->format("c");

            if (!empty($localItem->first_name)) {
                if (empty($updateItemContact->first_name)) {
                    $updateItemContact->first_name = $localItem->first_name;
                }
            }
            if (!empty($localItem->last_name)) {
                if (empty($updateItemContact->last_name)) {
                    $updateItemContact->last_name = $localItem->last_name;
                }
            }

            if (!empty($localItem->salutation)) {
                $updateItemContact->salutation = $localItem->salutation;
            }
            if (!empty($localItem->description)) {
                $updateItemContact->description = $localItem->description;
            }
            if (!empty($localItem->phone_work)) {
                $updateItemContact->phone_work = $localItem->phone_work;
            }
            if (!empty($localItem->phone_home)) {
                $updateItemContact->phone_home = $localItem->phone_home;
            }
            if (!empty($localItem->phone_fax)) {
                $updateItemContact->phone_fax = $localItem->phone_fax;
            }
            if (!empty($localItem->title)) {
                $updateItemContact->title = $localItem->title;
            }
        }

        //DECIDE OPERATION - CODES
        $operations["code"] = ($cachedItemContactCodes == $updateItemContactCodes) ? "skip" : $operations["code"];

        //DECIDE OPERATION - CODES
        $operations["contact"] = ($cachedItemContact == $updateItemContact) ? "skip" : $operations["contact"];


        //LOG
        if ($operations["contact"] != "skip") {
            $this->log("[" . $itemDb . "][" . json_encode($operations) . "][$identifiedBy]:");
            $this->log("LOCAL: " . json_encode($localItem));
            //$this->log("CODES: " . json_encode($cachedItemContactCodes));
            $this->log("CONTACT(C): " . json_encode($cachedItemContact));
            $this->log("CONTACT(U): " . json_encode($updateItemContact));

        }
        if (!empty($warnings)) {
            foreach ($warnings as $warning) {
                $this->log("WARNING: " . $warning);
            }
        }

        //CHECK
        if (!isset($operations["code"])) {
            throw new \Exception("operations[code] NOT SET!");
        }
        if (!isset($operations["contact"])) {
            throw new \Exception("operations[contact] NOT SET!");
        }
        /*
        if (empty($updateItemContact->first_name) && empty($updateItemContact->last_name)) {
            throw new \Exception("Both first and last names are empty!");
        }*/


        //INSERT / UPDATE CODES
        switch ($operations["code"]) {
            case "insert":
                $this->contactCodesCacheDb->addItem($updateItemContactCodes);
                break;
            case "update":
                $this->contactCodesCacheDb->updateItem($updateItemContactCodes);
                break;
            case "skip":
                break;
            default:
                throw new \Exception("Code operation(" . $operations["code"] . ") is not implemented!");
        }

        //INSERT / UPDATE CONTACT
        switch ($operations["contact"]) {
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
     * @param string $database IMP|MEKIT
     * @return bool|\stdClass
     *
     * @todo: importare anche quelli senza ruolo - come "RESP. CANTIERE"
     */
    protected function getNextLocalItem($database) {
        if (!$this->localItemStatement) {
            $db = Configuration::getDatabaseConnection("SERVER2K8");
            $sql = "SELECT
                TP.NOME AS first_name,
                TP.COGNOME AS last_name,
                TP.TITOLO AS salutation,
                TP.NOTE AS description,
                TP.EMAIL AS email,
                TP.CELL AS phone_mobile,
                TP.TELEFONO AS phone_work,
                TP.TELCASA AS phone_home,
                TP.FAX AS phone_fax,
                TP.POSIZIONE AS title,
                CASE WHEN NULLIF(TR.Descrizione, '') IS NOT NULL THEN TR.Descrizione ELSE 'NO-ROLE' END AS role,
                TP.RIFCODCONTO AS codiceMetodoAzienda,
                ACFR.CODCONTOFATT AS clienteDiFatturazione,
                CONCAT(TP.RIFCODCONTO COLLATE DATABASE_DEFAULT, '-' COLLATE DATABASE_DEFAULT, TP.CODICE COLLATE DATABASE_DEFAULT) AS metodoCombinedCode,
                TP.DATAMODIFICA AS DataDiModifica
                FROM [$database].[dbo].[TABELLAPERSONALE] AS TP
                LEFT JOIN [$database].[dbo].[TabellaRuoli] AS TR ON TP.CODRUOLO = TR.Codice
                INNER JOIN [$database].[dbo].[ANAGRAFICARISERVATICF] AS ACFR ON TP.RIFCODCONTO = ACFR.CODCONTO AND ACFR.ESERCIZIO = (SELECT TOP (1) TE.CODICE FROM [$database].[dbo].[TABESERCIZI] AS TE ORDER BY TE.CODICE DESC)
                WHERE (
                (NULLIF(TP.NOME, '') IS NOT NULL AND NULLIF(TP.COGNOME, '') IS NOT NULL) OR
                NULLIF(TP.EMAIL, '') IS NOT NULL OR
                NULLIF(TP.CELL, '') IS NOT NULL
                )
                ORDER BY TP.CELL DESC, TP.RIFCODCONTO ASC, TP.COGNOME DESC, TP.NOME DESC
                ";
            $this->localItemStatement = $db->prepare($sql);
            $this->localItemStatement->execute();
        }
        $item = $this->localItemStatement->fetch(\PDO::FETCH_OBJ);
        if ($item) {
            $item->database = $database;
            $item->first_name = strtoupper(trim($item->first_name));
            $item->last_name = strtoupper(trim($item->last_name));
            $item->salutation = $this->getSalutationFromCode((int) $item->salutation);
            $item->email = strtolower(trim($item->email));
            $item->phone_mobile = $this->normalizePhoneNumber($item->phone_mobile);
            $item->phone_work = $this->normalizePhoneNumber($item->phone_work);
            $item->phone_home = $this->normalizePhoneNumber($item->phone_home);
            $item->phone_fax = $this->normalizePhoneNumber($item->phone_fax);
            $item->title = trim($item->title);
            $item->role = trim($item->role);

            //normalization (set RUOLO as last_name if both first_name and last_name fields are empty)
            /*
            if (empty($item->first_name) && empty($item->last_name)) {
                $item->last_name = $item->role . ' - ' . $item->metodoCombinedCode;
            }
            */
            //
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