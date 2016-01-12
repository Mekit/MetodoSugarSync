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
use Mekit\SugarCrm\Rest\SugarCrmRest;
use Mekit\SugarCrm\Rest\SugarCrmRestException;
use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;

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
            $this->contactCacheDb->invalidateAll();
            $this->contactCodesCacheDb->invalidateAll();
        }

        if (isset($options["update-cache"]) && $options["update-cache"]) {
            $this->updateLocalCache();
            $this->cleanupLocalCache();
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

    /**
     * check and remove:
     * 1) contacts without codes:
     * SELECT C.id AS ORPHAN FROM Contact AS C LEFT OUTER JOIN Contact_Codes AS CC ON C.id = CC.contact_id WHERE CC.id IS NULL;
     *
     * 2) codes without corresponding contact:
     * SELECT CC.id AS ORPHAN FROM Contact_Codes AS CC LEFT OUTER JOIN Contact AS C ON CC.contact_id = C.id WHERE C.id IS NULL;
     *
     */
    protected function cleanupLocalCache() {

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
            $codeId = md5("CodeId-" . microtime(TRUE));
            //$contactId = md5("contactId-" . microtime(TRUE));
            //create codes
            $updateItemContactCodes = new \stdClass();
            $updateItemContactCodes->id = $codeId;
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
        if (empty($updateItemContact->first_name) && empty($updateItemContact->last_name)) {
            throw new \Exception("Both first and last names are empty!");
        }


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
        //
        $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
        //
        //$metodoLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->DataDiModifica);
        //$contact->metodo_last_update_time_c = $metodoLastUpdateTime->format("c");
        $contact->metodo_last_update_time_c = $oldDate->format("c");
        //
        //$crmLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
        //$contact->crm_last_update_time_c = $crmLastUpdateTime->format("c");
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
                ORDER BY TP.DATAMODIFICA ASC;
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
            if (empty($item->first_name) && empty($item->last_name)) {
                $item->last_name = $item->role . ' - ' . $item->metodoCombinedCode;
            }
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