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

    /** @ var  OrderLinesCache */
//    protected $orderLinesCacheDb;

    /** @var  \PDOStatement */
    protected $localItemStatement;

    /** @var array */
    protected $counters = [];

    /**
     * @param callable $logger
     */
    public function __construct($logger) {
        parent::__construct($logger);
        $this->orderCacheDb = new OrderCache('Orders', $logger);
        //$this->contactCodesCacheDb = new ContactCodesCache('Contact_Codes', $logger);
        $this->sugarCrmRest = new SugarCrmRest();
    }

    /**
     * @param array $options
     */
    public function execute($options) {
        //$this->log("EXECUTING..." . json_encode($options));
        if (isset($options["delete-cache"]) && $options["delete-cache"]) {
            $this->orderCacheDb->removeAll();
            //$this->orderLinesCacheDb->removeAll();
        }

        if (isset($options["invalidate-cache"]) && $options["invalidate-cache"]) {
            $this->orderCacheDb->invalidateAll(TRUE, TRUE);
        }

        if (isset($options["invalidate-local-cache"]) && $options["invalidate-local-cache"]) {
            $this->orderCacheDb->invalidateAll(TRUE, FALSE);
        }

        if (isset($options["invalidate-remote-cache"]) && $options["invalidate-remote-cache"]) {
            $this->orderCacheDb->invalidateAll(FALSE, TRUE);
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
                if ($this->counters["cache"]["index"] == 1) {
                    break;
                }
                $this->counters["cache"]["index"]++;
                $this->saveLocalItemInCache($localItem);
            }
        }
    }


    protected function updateRemoteFromCache() {
        $this->log("updating remote...");
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
        $itemDb = $localItem->database_metodo;

        $this->log(
            "-----------------------------------------------------------------------------------------"
            . $this->counters["cache"]["index"]
        );
        $this->log(json_encode($localItem));
        return;

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
            $contactCandidates = $this->orderCacheDb->loadItems($filter);
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
                $candidates = $this->orderCacheDb->loadItems($filter);
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
                    $cachedItemContact = $this->generateNewOrderObject($localItem);
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
            $candidates = $this->orderCacheDb->loadItems($filter);
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
            $candidates = $this->orderCacheDb->loadItems($filter);
            if ($candidates) {
                if (count($candidates) > 1) {
                    throw new \Exception("Search by Email returned multiple results!");
                }
                $cachedItemContact = $candidates[0];
                $operations["contact"] = 'update';
                $updateItemContact = clone($cachedItemContact);
                $updateItemContactCodes->contact_id = $updateItemContact->id;
            }
        }


        //all search failed - create new contact
        if (!$cachedItemContact) {
            $updateItemContact = $this->generateNewOrderObject($localItem);
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
                $this->orderCacheDb->addItem($updateItemContact);
                break;
            case "update":
                $this->orderCacheDb->updateItem($updateItemContact);
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
    protected function generateNewOrderObject($localItem) {
        $contact = new \stdClass();
        $contact->id = md5(json_encode($localItem) . microtime(TRUE));
        $contact->email = '[]';
        $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
        $contact->metodo_last_update_time_c = $oldDate->format("c");
        $contact->crm_last_update_time_c = $oldDate->format("c");
        return $contact;
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
}