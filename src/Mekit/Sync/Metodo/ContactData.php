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
        }

        if (isset($options["update-remote"]) && $options["update-remote"]) {
            $this->updateRemoteFromCache();
        }
    }

    /**
     *
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

    /**
     * @param \stdClass $localItem
     * @throws \Exception
     */
    protected function saveLocalItemInCache($localItem) {
        /** @var array|bool $operations */
        $operations = FALSE;

        /** @var \stdClass $cachedItemContact */
        $cachedItemContact = FALSE;

        /** @var \stdClass $cachedItemContactCodes */
        $cachedItemContactCodes = FALSE;

        /** @var string $identifiedBy */
        $identifiedBy = FALSE;

        /** @var array $warnings */
        $warnings = [];

        /** @var string $itemDb IMP|MEKIT */
        $itemDb = $localItem->database;


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
            $operations["code"] = 'update';
            //update role
            $cachedItemContactCodes->metodo_role = $localItem->role;
            //
            //if we have code get contact cache data right away
            $contactId = $cachedItemContactCodes->contact_id;
            $filter = [
                'id' => $contactId,
            ];
            $contactCandidates = $this->contactCacheDb->loadItems($filter);
            if ($contactCandidates && count($contactCandidates)) {
                $cachedItemContact = $contactCandidates[0];
                $operations["contact"] = 'update';
            }
        }
        else {
            //no code was found
            $codeId = md5("CodeId-" . microtime(TRUE));
            //$contactId = md5("contactId-" . microtime(TRUE));
            //create codes
            $cachedItemContactCodes = new \stdClass();
            $cachedItemContactCodes->id = $codeId;
            //$cachedItemContactCodes->contact_id = $contactId;
            $cachedItemContactCodes->database = $itemDb;
            $cachedItemContactCodes->metodo_contact_code = $localItem->metodoCombinedCode;
            $cachedItemContactCodes->metodo_code_company = $localItem->codiceMetodoAzienda;
            $cachedItemContactCodes->metodo_code_cf = $localItem->clienteDiFatturazione;
            $cachedItemContactCodes->metodo_role = $localItem->role;
            $operations["code"] = 'insert';
        }

        if (!$cachedItemContact) {
            //search by: mobile phone
            if (!empty($localItem->phone_mobile)) {
                $filter = [
                    'phone_mobile' => $localItem->phone_mobile
                ];
                $candidates = $this->contactCacheDb->loadItems($filter);
                if ($candidates) {
                    $this->log("FOUND CANDIDATES FOR MOBILE: " . json_encode($candidates));
                }

            }
        }


        //all search failed - create new
        if (!$cachedItemContact) {
            $cachedItemContact = new \stdClass();
            $contactId = md5("contactId-" . microtime(TRUE));
            $cachedItemContact->id = $contactId;
            $cachedItemContactCodes->contact_id = $contactId;
            //
            $cachedItemContact->email = '[]';
            //
            $metodoLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->DataDiModifica);
            $cachedItemContact->metodo_last_update_time_c = $metodoLastUpdateTime->format("c");

            $crmLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
            $cachedItemContact->crm_last_update_time_c = $crmLastUpdateTime->format("c");

            $operations["contact"] = 'insert';
        }

        $cachedItemContact->first_name = $localItem->first_name;
        $cachedItemContact->last_name = $localItem->last_name;

        //mobile
        if (!empty($localItem->phone_mobile)) {
            $cachedItemContact->phone_mobile = $localItem->phone_mobile;
        }

        //email
        $cachedItemContact->email =
            $this->addElementToJsonData($cachedItemContact->email, NULL, NULL, $localItem->email);

        //DECIDE OPERATION(better to keep this off for now)
        //$operation = ($cachedItem == $cacheUpdateItem) ? "skip" : $operation;

        //add other data on item
        $cachedItemContact->salutation = $localItem->salutation;
        $cachedItemContact->description = $localItem->description;
        $cachedItemContact->phone_work = $localItem->phone_work;
        $cachedItemContact->phone_home = $localItem->phone_home;
        $cachedItemContact->phone_fax = $localItem->phone_fax;
        $cachedItemContact->title = $localItem->title;


        if (!isset($operations["code"])) {
            throw new \Exception("operations[code] NOT SET!");
        }
        switch ($operations["code"]) {
            case "insert":
                $this->contactCodesCacheDb->addItem($cachedItemContactCodes);
                break;
            case "update":
                $this->contactCodesCacheDb->updateItem($cachedItemContactCodes);
                break;
            case "skip":
                break;
            default:
                throw new \Exception("Code operation(" . $operations["code"] . ") is not implemented!");
        }


        if (!isset($operations["contact"])) {
            throw new \Exception("operations[contact] NOT SET!");
        }
        switch ($operations["contact"]) {
            case "insert":
                $this->contactCacheDb->addItem($cachedItemContact);
                break;
            case "update":
                $this->contactCacheDb->updateItem($cachedItemContact);
                break;
            case "skip":
                break;
            default:
                throw new \Exception("Code operation(" . $operations["contact"] . ") is not implemented!");
        }
    }

    /**
     * @param \stdClass $localItem
     * @throws \Exception
     */
    protected function saveLocalItemInCacheOld($localItem) {
        /** @var string $operation */
        $operation = FALSE;

        /** @var \stdClass $cachedItemContact */
        $cachedItemContact = FALSE;
        /** @var \stdClass $cacheUpdateItemContact */
        $cacheUpdateItemContact = FALSE;

        /** @var \stdClass $cachedItemContactCodes */
        $cachedItemContactCodes = FALSE;
        /** @var \stdClass $cacheUpdateItemContactCodes */
        $cacheUpdateItemContactCodes = FALSE;

        /** @var string $identifiedBy */
        $identifiedBy = FALSE;
        /** @var array $warnings */
        $warnings = [];
        /** @var string $itemDb IMP|MEKIT */
        $itemDb = $localItem->database;

//        $this->log(str_repeat("-", 120));
//        $this->log("LOCAL ITEM(".$localItem->metodoCombinedCode."): " . json_encode($localItem));

        //@todo: tmp - only following codes!
        //$tmpFilter = ["C   603-1","C  2050-1","C  2889-1","C  3827-1","C  4010-1","C  4012-1"];
        $tmpFilter = ["C   603-1"];
        $tmpDb = "IMP";
        if ($itemDb != $tmpDb || !in_array($localItem->metodoCombinedCode, $tmpFilter)) {
            return;
        }


        //control by: mobile phone
        if (!$operation && !empty($localItem->phone_mobile)) {
            $filter = [
                'phone_mobile' => $localItem->phone_mobile
            ];
            $candidates = $this->contactCacheDb->loadItems($filter);
            if ($candidates) {
                //control if we have ClienteDiFatturazione
                foreach ($candidates as $candidate) {
                    $filter = [
                        'id' => $candidate->id,
                        'metodo_code_cf' => $localItem->clienteDiFatturazione
                    ];
                    $codeCandidates = $this->contactCacheDb->loadItems($filter);
                    if (count($codeCandidates)) {

                    }
                }


                //control if we have ClienteDiFatturazione
                foreach ($candidates as $candidate) {
                    if ($candidate->metodo_code_cf == $localItem->clienteDiFatturazione) {
                        $cachedItemContact = $candidate;
                        $operation = "update";
                        $identifiedBy = "MOBILE + CdF";
                        break;
                    }
                }

                //control by email
                if (!$operation) {
                    foreach ($candidates as $candidate) {
                        $emails = json_decode($candidate->email);
                        if (in_array($localItem->email, $emails)) {
                            $cachedItemContact = $candidate;
                            $operation = "update";
                            $identifiedBy = "MOBILE + Email";
                            break;
                        }
                    }
                }
            }

            if (!$operation) {
                $operation = "insert";
            }
        }


        //SKIP THE REST IF NO OPERATION - @todo: remove me
        if (!$operation) {
            $this->log("NO OPERATION!");
            return;
        }


        $this->log(str_repeat("-", 120) . $operation);
        $this->log("LOCAL ITEM(" . $localItem->metodoCombinedCode . "): " . json_encode($localItem));

        //create item for: update
        if ($operation == "update") {
            $cacheUpdateItemContact = clone($cachedItemContact);
            $metodoLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->DataDiModifica);
            $cachedDataDiModifica = new \DateTime($cachedItemContact->metodo_last_update_time_c);
            if ($metodoLastUpdateTime > $cachedDataDiModifica) {
                $cacheUpdateItemContact->metodo_last_update_time_c = $metodoLastUpdateTime->format("c");
            }
        }

        //create item for: insert
        if ($operation == "insert") {
            $cacheUpdateItemContact = new \stdClass();
            $id = md5($localItem->first_name . $localItem->last_name . "-" . microtime(TRUE));
            $cacheUpdateItemContact->id = $id;
            $cacheUpdateItemContact->parent_id = $id;

            $cacheUpdateItemContact->email = '[]';
            $cacheUpdateItemContact->database = $itemDb;
//            $cacheUpdateItem->metodo_codes_contact = '{}';
//            $cacheUpdateItem->metodo_codes_cf = '{}';
//            $cacheUpdateItem->metodo_roles = '{}';

            $metodoLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->DataDiModifica);
            $cacheUpdateItemContact->metodo_last_update_time_c = $metodoLastUpdateTime->format("c");

            $crmLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
            $cacheUpdateItemContact->crm_last_update_time_c = $crmLastUpdateTime->format("c");
        }

        //add shared data on item (ONLY CODES)
        $cacheUpdateItemContact->first_name = $localItem->first_name;
        $cacheUpdateItemContact->last_name = $localItem->last_name;


        if (!empty($localItem->phone_mobile)) {
            $cacheUpdateItemContact->phone_mobile = $localItem->phone_mobile;
        }

        //Email
        $cacheUpdateItemContact->email =
            $this->addElementToJsonData($cacheUpdateItemContact->email, NULL, NULL, $localItem->email);

        //DECIDE OPERATION(better to keep this off for now)
        //$operation = ($cachedItem == $cacheUpdateItem) ? "skip" : $operation;

        //add other data on item
        $cacheUpdateItemContact->salutation = $localItem->salutation;
        $cacheUpdateItemContact->description = $localItem->description;
        $cacheUpdateItemContact->phone_work = $localItem->phone_work;
        $cacheUpdateItemContact->phone_home = $localItem->phone_home;
        $cacheUpdateItemContact->phone_fax = $localItem->phone_fax;
        $cacheUpdateItemContact->title = $localItem->title;
        //
        $cacheUpdateItemContact->metodo_contact_code = $localItem->metodoCombinedCode;
        $cacheUpdateItemContact->metodo_code_company = $localItem->codiceMetodoAzienda;
        $cacheUpdateItemContact->metodo_code_cf = $localItem->clienteDiFatturazione;
        $cacheUpdateItemContact->metodo_role = $localItem->role;



        if ($operation != "skip") {
            $this->log(
                "-----------------------------------------------------------------------------------------"
                . $this->counters["cache"]["index"]
            );
            $this->log(
                "[" . $itemDb . "][$operation][$identifiedBy]-"
                . "[" . $localItem->first_name . "]"
                . "[" . $localItem->last_name . "]"
            );
            $this->log("CACHED: " . json_encode($cachedItemContact));
            $this->log("UPDATE: " . json_encode($cacheUpdateItemContact));
        }
        if (!empty($warnings)) {
            foreach ($warnings as $warning) {
                $this->log("WARNING: " . $warning);
            }
        }

        switch ($operation) {
            case "insert":
                $this->contactCacheDb->addItem($cacheUpdateItemContact);
                $this->contactCodesCacheDb->addItem($cacheUpdateItemContact);
                break;
            case "update":
                //$this->contactCacheDb->updateItem($cacheUpdateItem);
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
                TR.Descrizione AS role,
                TP.RIFCODCONTO AS codiceMetodoAzienda,
                ACFR.CODCONTOFATT AS clienteDiFatturazione,
                CONCAT(TP.RIFCODCONTO COLLATE DATABASE_DEFAULT, '-' COLLATE DATABASE_DEFAULT, TP.CODICE COLLATE DATABASE_DEFAULT) AS metodoCombinedCode,
                TP.DATAMODIFICA AS DataDiModifica
                FROM [$database].[dbo].[TABELLAPERSONALE] AS TP
                INNER JOIN [$database].[dbo].[TabellaRuoli] AS TR ON TP.CODRUOLO = TR.Codice
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