<?php
/**
 * Created by Adam Jakab.
 * Date: 01/12/15
 * Time: 11.32
 */

namespace Mekit\Sync\Metodo\Down;


use Mekit\Console\Configuration;
use Mekit\DbCache\AccountCache;
use Mekit\Sync\SugarCrm\Rest\SugarCrmRest;
use Mekit\Sync\SugarCrm\Rest\SugarCrmRestException;

class AccountDataOld {
    /** @var callable */
    protected $logger;

    /** @var SugarCrmRest */
    protected $sugarCrmRest;

    /** @var  AccountCache */
    protected $cacheDb;

    /** @var string */
    protected $dataIdentifier = 'Account';

    /** @var array */
    protected $index = [];

    /**
     * @param callable $logger
     */
    public function __construct($logger) {
        $this->logger = $logger;
        $this->cacheDb = new AccountCache($this->dataIdentifier, $logger);
        $this->sugarCrmRest = new SugarCrmRest();
    }

    public function execute() {
        $this->log("getting account data from Metodo...");
        $this->loadIndex();
        $this->log("Total Items: " . count($this->index));
        $this->elaborateIndexItems();
    }

    protected function elaborateIndexItems() {
        $i = 0;
        $maxRun = 1;//999999999;
        while ($indexItem = array_pop($this->index)) {
            if ($i >= $maxRun) {
                $this->log("Reached hard limit($maxRun).");
                break;
            }

            $this->log(
                "------------------------------------------------------------($i): '" . $indexItem["CodiceMetodo"]
                . "' - " . $indexItem["Nome1"]
            );
            $localItem = $this->loadLocalItem($indexItem);
            $this->log("Local item(metodo): " . json_encode($localItem));
            $cacheItem = $this->loadCacheItemByCodiceMetodo($localItem);
            $this->log("Cache item: " . json_encode($cacheItem));

//            $remoteItem = $this->loadRemoteItem($indexItem);
//            $this->log("Remote item: " . json_encode($remoteItem));
//            $syncItem = $this->createSyncItem($localItem, $remoteItem);
//            $this->log("Sync item: " . json_encode($syncItem));
//            $savedItem = $this->saveRemoteItem($syncItem);
//            $this->log("Saved item: " . json_encode($savedItem));
//            $this->cacheSavedItem($savedItem, "IMP", $localItem);
            $i++;
        }
    }

    /**
     * @param \stdClass|bool $savedItem
     * @param string         $database
     * @param \stdClass      $localItem
     */
    protected function cacheSavedItem($savedItem, $database = "IMP", $localItem) {
        if ($savedItem) {
            $filter = [];
            if (!empty($savedItem->partita_iva_c) && $savedItem->partita_iva_c != '00000000000') {
                $filter["partita_iva_c"] = $savedItem->partita_iva_c;
            }
            if (!count($filter) && !empty($savedItem->codice_fiscale_c)) {
                $filter["codice_fiscale_c"] = $savedItem->codice_fiscale_c;
            }
            if (!count($filter)) {
                $remoteFieldNameForCodiceMetodo = $this->getRemoteFieldNameForCodiceMetodo($database, $localItem->Tipologia);
                $filter[$remoteFieldNameForCodiceMetodo] = $localItem->CodiceMetodo;
            }
            if (!$this->cacheDb->loadItem($filter)) {
                $this->cacheDb->addItem($savedItem);
            }
            else {
                $this->cacheDb->updateItem($savedItem);
            }
        }
    }


    /**
     * @param Array|bool $syncItem
     * @return \stdClass|bool
     */
    protected function saveRemoteItem($syncItem) {
        $result = FALSE;
        if ($syncItem) {
            if (isset($syncItem["id"])) {
                //UPDATE
                $this->log("updating...: " . json_encode($syncItem));
                $id = $syncItem["id"];
                unset($syncItem["id"]);
                try {
                    $result = $this->sugarCrmRest->comunicate('/Accounts/' . $id, 'PUT', $syncItem);
                } catch(SugarCrmRestException $e) {
                    //go ahead with false silently
                    $this->log("ERROR SAVING!!! - " . $e->getMessage());
                }
            }
            else {
                //CREATE
                $this->log("creating...");
                try {
                    $result = $this->sugarCrmRest->comunicate('/Accounts', 'POST', $syncItem);
                } catch(SugarCrmRestException $e) {
                    //go ahead with false silently
                }
            }
        }
        else {
            $this->log("skipping...");
        }
        return $result;
    }


    /**
     * @param bool|\stdClass $localItem
     * @param bool|\stdClass $remoteItem
     * @return bool|Array
     * @throws \Exception
     */
    protected function createSyncItem($localItem, $remoteItem) {
        $syncItem = FALSE;
        if ($localItem) {
            $isUpToDate = FALSE;
            $remoteFieldNameForCodiceMetodo = $this->getRemoteFieldNameForCodiceMetodo($localItem->database, $localItem->Tipologia);


            if (FALSE && $remoteItem) {
                //confront local(DataDiModifica) and remote(metodo_last_update_time_c) dates
                //Skip ONLY if CodiceMetodo IS present on remoteItem
                $remoteCode = trim($remoteItem->$remoteFieldNameForCodiceMetodo);
                //$this->log("Remote Code($remoteFieldNameForCodiceMetodo): " . $remoteCode);
                if ($remoteCode) {
                    $localDataDiModifica = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->DataDiModifica);
                    //remove seconds from local date
                    $localDataDiModifica->setTime((int) $localDataDiModifica->format("G"), (int) $localDataDiModifica->format("i"), 0);
                    //
                    $remoteDataDiModifica = new \DateTime($remoteItem->metodo_last_update_time_c);
                    //$this->log("Local mod date: " . $localDataDiModifica->format("c"));
                    //$this->log("Remote mod date: " . $remoteDataDiModifica->format("c"));
                    if ($localDataDiModifica > $remoteDataDiModifica) {
                        $isUpToDate = FALSE;
                        //$this->log("Needs update!");
                    }
                    else {
                        $isUpToDate = TRUE;
                        //$this->log("No update needed!");
                    }
                }
            }

            if (!$isUpToDate) {
                $syncItem = [];

                //Add remote id to sync item so it will be updated
                if ($remoteItem) {
                    $syncItem["id"] = $remoteItem->id;
                    //@todo: map other stuff?
                }

                //Codice Metodo
                $syncItem[$remoteFieldNameForCodiceMetodo] = $localItem->CodiceMetodo;

                //Data di Modifica (2011-03-23 17:13:58.180)
                $dataDiModifica = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->DataDiModifica);
                $syncItem["metodo_last_update_time_c"] = $dataDiModifica->format("c");

                //Other Data
                $syncItem["name"] = $localItem->Nome1 . (!empty($localItem->Nome2) ? ' - ' . $localItem->Nome2 : '');
                $syncItem["codice_fiscale_c"] = $localItem->CodiceFiscale;
                $syncItem["partita_iva_c"] = $localItem->PartitaIva;
            }
        }
        return $syncItem;
    }

    /**
     * @param string $database
     * @param string $type
     * @return string
     * @throws \Exception
     */
    protected function getRemoteFieldNameForCodiceMetodo($database, $type) {
        switch ($database) {
            case "IMP":
                switch ($type) {
                    case "C":
                        $answer = "metodo_client_code_imp_c";
                        break;
                    case "F":
                        $answer = "metodo_supplier_code_imp_c";
                        break;
                    default:
                        throw new \Exception("Local item needs to have Tipologia C|F!");
                }
                break;
            case "MEKIT":
                switch ($type) {
                    case "C":
                        $answer = "metodo_client_code_mekit_c";
                        break;
                    case "F":
                        $answer = "metodo_supplier_code_mekit_c";
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
     * @param \stdClass $localItem
     * @return bool|mixed
     * @throws \Exception
     */
    protected function loadCacheItemByCodiceMetodo($localItem) {
        $filter = [];
        $remoteFieldNameForCodiceMetodo = $this->getRemoteFieldNameForCodiceMetodo($localItem->database, $localItem->Tipologia);
        $filter[$remoteFieldNameForCodiceMetodo] = $localItem->CodiceMetodo;
        return $this->cacheDb->loadItem($filter);
    }

    /**
     * @param array $localItem
     * @return \stdClass|bool
     */
    protected function loadRemoteItem($localItem) {
        $itemData = FALSE;

        /*
        $cacheFileName = $this->getCachedFilePath($item["CodiceMetodo"]);
        if (file_exists($cacheFileName)) {
            $itemData = unserialize(file_get_contents($cacheFileName));
        }
        */

        if (!$itemData) {
            $filter = [];
            if (!empty($localItem["PartitaIva"]) && $localItem["PartitaIva"] != '00000000000') {
                $filter[] = ["partita_iva_c" => $localItem["PartitaIva"]];
            }
            if (!count($filter) && !empty($localItem["CodiceFiscale"])) {
                $filter[] = ["codice_fiscale_c" => $localItem["CodiceFiscale"]];
            }
            if (!count($filter)) {
                $remoteFieldNameForCodiceMetodo = $this->getRemoteFieldNameForCodiceMetodo($localItem["database"], $localItem["Tipologia"]);
                $filter[] = [$remoteFieldNameForCodiceMetodo => $localItem["CodiceMetodo"]];
            }

            if (count($filter)) {
                $arguments = [
                    "filter" => $filter,
                    "max_num" => 1,
                    "offset" => 0,
                    "fields" => implode(
                        ",", [
                               "id",
                               "metodo_last_update_time_c",
                               "metodo_client_code_imp_c",
                               "metodo_supplier_code_imp_c",
                               "metodo_client_code_mekit_c",
                               "metodo_supplier_code_mekit_c"
                           ]
                    ),
                ];
                try {
                    $result = $this->sugarCrmRest->comunicate('/Accounts/filter', 'GET', $arguments);
                    if (isset($result->records) && count($result->records)) {
                        $itemData = $result->records[0];
                    }
                } catch(SugarCrmRestException $e) {
                    //go ahead silently with false
                }
            }
        }

        return ($itemData);
    }

    /**
     * For now $item is sufficient for local item - if not here you can execute additional loads
     *
     * @param array $item
     * @return \stdClass
     */
    protected function loadLocalItem($item) {
        $itemData = (object) $item;
        return ($itemData);
    }

    /**
     * @param string $database IMP|MEKIT
     */
    protected function loadIndex($database = "IMP") {
        $db = Configuration::getDatabaseConnection("SERVER2K8");
        $query = "SELECT * FROM [$database].[dbo].[ABJ_CLI_FOR_INDEX]";
        $statement = $db->prepare($query);
        $statement->execute();
        $index = $statement->fetchAll(\PDO::FETCH_ASSOC);
        foreach ($index as &$item) {
            $item["CodiceMetodo"] = trim($item["CodiceMetodo"]);
            $item["database"] = $database;
        }
        $this->index = $index;
    }

    /**
     * @param string $msg
     */
    protected function log($msg) {
        call_user_func($this->logger, $msg);
    }
}