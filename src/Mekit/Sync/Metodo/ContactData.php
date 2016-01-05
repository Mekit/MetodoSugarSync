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
                $this->counters["cache"]["index"]++;
                //$this->saveLocalItemInCache($localItem);
                $this->log("ITEM(" . $this->counters["cache"]["index"] . ") - " . json_encode($localItem));
            }
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
            $item->phone_mobile = trim($item->phone_mobile);
            $item->phone_work = trim($item->phone_work);
            $item->phone_home = trim($item->phone_home);
            $item->phone_fax = trim($item->phone_fax);
            $item->title = trim($item->title);
            $item->ruolo = trim($item->ruolo);
            //normalization
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