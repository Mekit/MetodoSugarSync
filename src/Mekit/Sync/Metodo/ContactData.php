<?php
/**
 * Created by Adam Jakab.
 * Date: 05/01/16
 * Time: 14.34
 */

namespace Mekit\Sync\Metodo;

use Mekit\Console\Configuration;
use Mekit\DbCache\ContactCache;
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
    protected $cacheDb;

    /** @var string */
    protected $dataIdentifier = 'Contact';

    /** @var  \PDOStatement */
    protected $localItemStatement;

    /** @var array */
    protected $counters = [];

    /**
     * @param callable $logger
     */
    public function __construct($logger) {
        parent::__construct($logger);
        $this->cacheDb = new ContactCache($this->dataIdentifier, $logger);
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
            $this->cacheDb->invalidateAll();
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
//                if ($this->counters["cache"]["index"] == 300) {
//                    break;
//                }
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
        /** @var string $operation */
        $operation = FALSE;
        /** @var \stdClass $cachedItem */
        $cachedItem = FALSE;
        /** @var \stdClass $cacheUpdateItem */
        $cacheUpdateItem = FALSE;
        /** @var string $identifiedBy */
        $identifiedBy = FALSE;
        /** @var array $warnings */
        $warnings = [];

        //control by: CellPhone + (firstName or lastName)
        if (!$operation && !empty($localItem->phone_mobile)
            /*&& (!empty($localItem->first_name) || !empty($localItem->last_name))*/
        ) {

            $filter = [
                'phone_mobile' => $localItem->phone_mobile
            ];
            /*
            if (!empty($localItem->first_name)) {
                $filter['first_name'] = $localItem->first_name;
            }
            if (!empty($localItem->last_name)) {
                $filter['last_name'] = $localItem->last_name;
            }*/
            $candidates = $this->cacheDb->loadItems($filter);
            if ($candidates) {
                if (count($candidates) > 1) {
                    $warnings[] = "-----------------------------------------------------------------------------------------";
                    $warnings[] = "MULTIPLE CACHED ITEMS FOUND FOR MOBILE + FIRSTNAME/LASTNAME COMBINATION["
                                  . $localItem->database . "]: "
                                  . $localItem->phone_mobile
                                  . " - "
                                  . $localItem->first_name
                                  . " "
                                  . $localItem->last_name;
                }
                $candidate = $candidates[0];
                $cachedItem = $candidate;
                $operation = "update";
                $identifiedBy = "MOBILE + FIRSTNAME/LASTNAME";
            }
        }

        //control by: Email + (firstName or lastName)
        if (!$operation && !empty($localItem->email)
            /*&& (!empty($localItem->first_name) || !empty($localItem->last_name))*/
        ) {
            $filter = [
                'email' => $localItem->email
            ];
            /*
            if (!empty($localItem->first_name)) {
                $filter['first_name'] = $localItem->first_name;
            }
            if (!empty($localItem->last_name)) {
                $filter['last_name'] = $localItem->last_name;
            }*/
            $candidates = $this->cacheDb->loadItems($filter);
            if ($candidates) {
                if (count($candidates) > 1) {
                    $warnings[] = "-----------------------------------------------------------------------------------------";
                    $warnings[] = "MULTIPLE CACHED ITEMS FOUND FOR EMAIL + FIRSTNAME/LASTNAME COMBINATION["
                                  . $localItem->database . "]: "
                                  . $localItem->email
                                  . " - "
                                  . $localItem->first_name
                                  . " "
                                  . $localItem->last_name;
                }
                $candidate = $candidates[0];
                $cachedItem = $candidate;
                $operation = "update";
                $identifiedBy = "EMAIL + FIRSTNAME/LASTNAME";
            }
        }

        //control by: FIRSTNAME + LASTNAME
        if (!empty($localItem->first_name) && !empty($localItem->last_name)) {
            $filter = [
                'first_name' => $localItem->first_name,
                'last_name' => $localItem->last_name
            ];
            $candidates = $this->cacheDb->loadItems($filter);
            if ($candidates) {
                if (count($candidates) > 1) {
                    $warnings[] = "-----------------------------------------------------------------------------------------";
                    $warnings[] = "MULTIPLE CACHED ITEMS FOUND FOR FIRSTNAME + LASTNAME COMBINATION["
                                  . $localItem->database . "]: "
                                  . $localItem->first_name
                                  . " "
                                  . $localItem->last_name;
                }
                $candidate = $candidates[0];
                $cachedItem = $candidate;
                $operation = "update";
                $identifiedBy = "FIRSTNAME + LASTNAME";
            }
        }

        if (!$operation) {
            $operation = "insert";
            $identifiedBy = "NOPE";
        }


        //create item for: update
        if ($operation == "update") {
            $cacheUpdateItem = clone($cachedItem);
            $metodoLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->DataDiModifica);
            $cachedDataDiModifica = new \DateTime($cachedItem->metodo_last_update_time_c);
            if ($metodoLastUpdateTime > $cachedDataDiModifica) {
                $cacheUpdateItem->metodo_last_update_time_c = $metodoLastUpdateTime->format("c");
            }
        }

        //create item for: insert
        if ($operation == "insert") {
            $cacheUpdateItem = new \stdClass();
            $cacheUpdateItem->id = md5($localItem->first_name . $localItem->last_name . "-" . microtime(TRUE));

            $cacheUpdateItem->email = json_encode([]);
            $cacheUpdateItem->phone_mobile = json_encode([]);

            $metodoLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->DataDiModifica);
            $cacheUpdateItem->metodo_last_update_time_c = $metodoLastUpdateTime->format("c");

            $crmLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
            $cacheUpdateItem->crm_last_update_time_c = $crmLastUpdateTime->format("c");
        }

        //add shared data on item (ONLY CODES)
        $cacheUpdateItem->first_name = $localItem->first_name;
        $cacheUpdateItem->last_name = $localItem->last_name;

        if (!empty($localItem->phone_mobile)) {
            $mobiles = json_decode($cacheUpdateItem->phone_mobile);
            if (!in_array($localItem->phone_mobile, $mobiles)) {
                $mobiles[] = $localItem->phone_mobile;
                $cacheUpdateItem->phone_mobile = json_encode($mobiles);
            }
        }

        if (!empty($localItem->email)) {
            $emails = json_decode($cacheUpdateItem->email);
            if (!in_array($localItem->email, $emails)) {
                $emails[] = $localItem->email;
                $cacheUpdateItem->email = json_encode($emails);
            }
        }

        //DECIDE OPERATION(better to keep this off for now)
        $operation = ($cachedItem == $cacheUpdateItem) ? "skip" : $operation;

        //add other data on item


        if ($operation != "skip") {
            $this->log(
                "-----------------------------------------------------------------------------------------"
                . $this->counters["cache"]["index"]
            );
            $this->log(
                "[" . $localItem->database . "][$operation][$identifiedBy]-"
                . "[" . $localItem->first_name . "]"
                . "[" . $localItem->last_name . "]"
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
                TR.Descrizione AS ruolo,
                TP.RIFCODCONTO AS codiceAnagrafica,
                TP.DATAMODIFICA AS DataDiModifica
                FROM [$database].[dbo].[TABELLAPERSONALE] AS TP
                INNER JOIN [$database].[dbo].[TabellaRuoli] AS TR ON TP.CODRUOLO = TR.Codice
                WHERE (
                (NULLIF(TP.NOME, '') IS NOT NULL AND NULLIF(TP.COGNOME, '') IS NOT NULL) OR
                NULLIF(TP.EMAIL, '') IS NOT NULL OR
                NULLIF(TP.CELL, '') IS NOT NULL
                )
                ORDER BY TP.DATAMODIFICA ASC;
                ";
            $this->localItemStatement = $db->prepare($sql);
            $this->localItemStatement->execute();


//            if(!empty($this->localItemStatement->errorInfo())) {
//                throw new \Exception("MSSQL(ERR): " . json_encode($this->localItemStatement->errorInfo()));
//            }

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
            $item->ruolo = trim($item->ruolo);

            //normalization (set RUOLO as last_name if both first_name and last_name fields are empty)
            if (empty($item->first_name) && empty($item->last_name)) {
                $item->last_name = $item->ruolo;
            }
            //
        }
        else {
            $this->localItemStatement = NULL;
        }
        return $item;
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