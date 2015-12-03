<?php
/**
 * Created by Adam Jakab.
 * Date: 01/12/15
 * Time: 11.32
 */

namespace Mekit\Sync\Metodo\Down;


use Mekit\Console\Configuration;
use Mekit\Sync\SugarCrm\Rest\SugarCrmRest;
use Mekit\Sync\SugarCrm\Rest\SugarCrmRestException;

class AccountData {
    /** @var callable */
    protected $logger;

    /** @var SugarCrmRest */
    protected $sugarCrmRest;

    /** @var array  */
    protected $index = [];

    /**
     * @param callable $logger
     */
    public function __construct($logger) {
        $this->logger = $logger;
        $this->sugarCrmRest = new SugarCrmRest();
        $this->createCacheDir();
    }

    //$query = "SELECT TOP 10 * FROM [SogCRM_TesteDocumenti] WHERE [TipoDoc] = 'DDT'";
    public function execute() {
        $this->log("getting account data from Metodo...");
        $this->loadIndex();
        $this->log("Total Items: " . count($this->index));
        $this->elaborateIndexItems();
    }

    protected function elaborateIndexItems() {
        $i = 0;
        $maxRun = 10;//999999999;
        //foreach($this->index as $indexItem) {
        while($indexItem = array_pop($this->index)) {
            if($i >= $maxRun) {
                $this->log("Reached hard limit($maxRun).");
                break;
            }
//            if($indexItem["Nome1"] != "MEKIT SCS") {
//                continue;
//            }

            $this->log(
                "------------------------------------------------------------($i): '" . $indexItem["CodiceMetodo"]
                . "' - " . $indexItem["Nome1"]
            );
            $localItem = $this->loadLocalItem($indexItem);
            //$this->log("Local item: " . json_encode($localItem));
            $remoteItem = $this->loadRemoteItem($indexItem);
            //$this->log("Remote item: " . json_encode($remoteItem));
            $syncItem = $this->createSyncItem($localItem, $remoteItem);
            //$this->log("Sync item: " . json_encode($syncItem));
            $savedItem = $this->saveRemoteItem($syncItem);
            $this->log("Saved item: " . json_encode($savedItem));
            //@todo - saved item should be serialized and saved so next time we don't load it from remote
            $this->cacheSavedItem(NULL, $savedItem);
            $i++;
        }
    }

    /**
     * @param string         $codiceMetodo
     * @param \stdClass|bool $savedItem
     */
    protected function cacheSavedItem($codiceMetodo, $savedItem) {
        if ($savedItem) {
            $codeFieldNames = [
                "metodo_client_code_imp_c",
                "metodo_supplier_code_imp_c",
                "metodo_client_code_mekit_c",
                "metodo_supplier_code_mekit_c"
            ];
            foreach ($codeFieldNames as $codeFieldName) {
                if (isset($savedItem->$codeFieldName) && !empty($savedItem->$codeFieldName)) {
                    $cacheFileName = $this->getCachedFilePath($savedItem->$codeFieldName);
                    //$this->log("creating cache file: " . $cacheFileName);
                    file_put_contents($cacheFileName, serialize($savedItem));
                }
            }
        }
    }

    /**
     * @param string $codiceMetodo
     * @return string
     */
    protected function getCachedFilePath($codiceMetodo) {
        $cfg = Configuration::getConfiguration();
        return $cfg["global"]["temporary_path"] . '/'
               . 'Account' . '/'
               . $this->normalizeCodiceMetodo($codiceMetodo)
               . '.ser';
    }

    /**
     * Create cache directory for this Module
     */
    protected function createCacheDir() {
        $cacheDir = dirname($this->getCachedFilePath("cache"));
        if (!is_dir($cacheDir)) {
            $this->log("creating cache dir: " . $cacheDir);
            mkdir($cacheDir, 0775, TRUE);
        }
    }


    /**
     * @param string $codiceMetodo
     * @return string
     */
    protected function normalizeCodiceMetodo($codiceMetodo) {
        return strtoupper((str_replace(" ", "_", $codiceMetodo)));
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
        } else {
            $this->log("skipping...");
        }
        return $result;
    }


    /**
     * @param bool|\stdClass    $localItem
     * @param bool|\stdClass    $remoteItem
     * @return bool|Array
     * @throws \Exception
     */
    protected function createSyncItem($localItem, $remoteItem) {
        $syncItem = false;
        if($localItem) {
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
     * @param array $item
     * @return \stdClass|bool
     */
    protected function loadRemoteItem($item) {
        $itemData = false;

        $cacheFileName = $this->getCachedFilePath($item["CodiceMetodo"]);
        if (file_exists($cacheFileName)) {
            $itemData = unserialize(file_get_contents($cacheFileName));
        }

        if (!$itemData) {
            $orFilter = [];
            if (!empty($item["PartitaIva"]) && $item["PartitaIva"] != '00000000000') {
                $orFilter[] = ["partita_iva_c" => $item["PartitaIva"]];
            }
            if (!empty($item["CodiceFiscale"])) {
                $orFilter[] = ["codice_fiscale_c" => $item["CodiceFiscale"]];
            }
            $orFilter[] = ["metodo_client_code_imp_c" => $item["CodiceMetodo"]];
            $orFilter[] = ["metodo_supplier_code_imp_c" => $item["CodiceMetodo"]];
            $orFilter[] = ["metodo_client_code_mekit_c" => $item["CodiceMetodo"]];
            $orFilter[] = ["metodo_supplier_code_mekit_c" => $item["CodiceMetodo"]];

            $arguments = [
                "filter" => [
                    [
                        '$or' => $orFilter,
                    ]
                ],
                "max_num" => 1,
                "offset" => 0,
                "fields" => "id,metodo_last_update_time_c,metodo_client_code_imp_c,metodo_supplier_code_imp_c,metodo_client_code_mekit_c,metodo_supplier_code_mekit_c",
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

        return($itemData);
    }

    /**
     * For now $item is sufficient for local item
     *
     * @param array $item
     * @return \stdClass
     */
    protected function loadLocalItem($item) {
        /*
        $db = Configuration::getDatabaseConnection("SERVER2K8");
        $query = "SELECT * FROM [IMP].[dbo].[SogCRM_AnagraficaCF] WHERE [CodiceMetodo] = '" . $item["CodiceMetodo"] . "'";
        $statement = $db->prepare($query);
        $statement->execute();
        $itemData = $statement->fetch(\PDO::FETCH_OBJ);
        */
        $itemData = (object)$item;
        return($itemData);
    }

    /**
     * @param string $database IMP|MEKIT
     */
    protected function loadIndex($database="IMP") {
        $db = Configuration::getDatabaseConnection("SERVER2K8");
        $query = "SELECT * FROM [$database].[dbo].[ABJ_CLI_FOR_INDEX]";
        $statement = $db->prepare($query);
        $statement->execute();
        $index = $statement->fetchAll(\PDO::FETCH_ASSOC);
        foreach($index as &$item) {
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