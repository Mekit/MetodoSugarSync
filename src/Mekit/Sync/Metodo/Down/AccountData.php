<?php
/**
 * Created by Adam Jakab.
 * Date: 01/12/15
 * Time: 11.32
 */

namespace Mekit\Sync\Metodo\Down;


use Mekit\Console\Configuration;
use Mekit\Sync\SugarCrm\Rest\SugarCrmRest;

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
    }

    //$query = "SELECT TOP 10 * FROM [SogCRM_TesteDocumenti] WHERE [TipoDoc] = 'DDT'";
    public function execute() {
        $this->log("getting sugarcrm auth...");
        $this->log("getting account data from Metodo...");
        $this->loadIndex();
        $this->log("Totale Items: " . count($this->index));
        $this->elaborateIndexItems();
    }

    protected function elaborateIndexItems() {
        $i = 0;
        $maxRun = 999999999;
        //foreach($this->index as $indexItem) {
        while($indexItem = array_pop($this->index)) {
            if($i >= $maxRun) {
                $this->log("Reached hard limit($maxRun).");
                break;
            }
//            if($indexItem["Nome1"] != "MEKIT SCS") {
//                continue;
//            }

            $this->log("------------------------------------------------------------($i):" . $indexItem["CodiceMetodo"]);
            $localItem = $this->loadLocalItem($indexItem);
            //$this->log("Local item: " . json_encode($localItem));
            $remoteItem = $this->loadRemoteItem($indexItem);
            //$this->log("Remote item: " . json_encode($remoteItem));
            $syncItem = $this->createSyncItem($localItem, $remoteItem);
            //$this->log("Sync item: " . json_encode($syncItem));
            $savedItem = $this->saveRemoteItem($syncItem);
            //$this->log("Saved item: " . json_encode($savedItem));
            $i++;
        }
    }

    /**
     * @param Array $syncItem
     * @return \stdClass
     */
    protected function saveRemoteItem($syncItem) {
        if(isset($syncItem["id"])) {
            //UPDATE
            $this->log("updating...");
            $id = $syncItem["id"];
            unset($syncItem["id"]);
            $result = $this->sugarCrmRest->comunicate('/Accounts/' . $id, 'PUT', $syncItem);
        } else {
            //CREATE
            $this->log("creating...");
            $result = $this->sugarCrmRest->comunicate('/Accounts', 'POST', $syncItem);
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
            $syncItem = [];
            if($remoteItem) {
                //@todo: confront local(DataDiModifica) and remote(metodo_last_update_time_c) dates
                //Skip ONLY if CodiceMetodo iIS present on remoteItem
                if(true) {
                    //local items has more recent date
                    $syncItem["id"] = $remoteItem->id;
                    //@todo: map other stuff?

                }


            }
            //CODICE METODO
            switch($localItem->database) {
                case "IMP":
                    switch($localItem->Tipologia) {
                        case "C":
                            $syncItem["metodo_client_code_imp_c"] = $localItem->CodiceMetodo;
                            break;
                        case "F":
                            $syncItem["metodo_supplier_code_imp_c"] = $localItem->CodiceMetodo;
                            break;
                        default:
                            throw new \Exception("Local item needs to have Tipologia C|F!");
                    }
                    break;
                case "MEKIT":
                    switch($localItem->Tipologia) {
                        case "C":
                            $syncItem["metodo_client_code_mekit_c"] = $localItem->CodiceMetodo;
                            break;
                        case "F":
                            $syncItem["metodo_supplier_code_mekit_c"] = $localItem->CodiceMetodo;
                            break;
                        default:
                            throw new \Exception("Local item needs to have Tipologia C|F!");
                    }
                    break;
                default:
                    throw new \Exception("Local item needs to have database IMP|MEKIT!");
            }

            $syncItem["name"] = $localItem->Nome1 . (!empty($localItem->Nome2) ? ' - ' . $localItem->Nome2 : '');
            $syncItem["codice_fiscale_c"] = $localItem->CodiceFiscale;
            $syncItem["partita_iva_c"] = $localItem->PartitaIva;


//            $syncItem["sync_codes_c"] = $localItem->CodiceMetodo;
//            $syncItem["codice_fiscale_c"] = $localItem->CodiceFiscale;
//            $syncItem["partita_iva_c"] = $localItem->PartitaIVA;
        }
        return $syncItem;
    }

    /**
     * @param array $item
     * @return \stdClass|bool
     */
    protected function loadRemoteItem($item) {
        $itemData = false;
        $arguments = [
            "filter" => [
                [
                    '$or' => [
                                 ["partita_iva_c" => $item["PartitaIva"]],
                                 ["codice_fiscale_c" => $item["CodiceFiscale"]],
                                 ["metodo_client_code_imp_c" => $item["CodiceMetodo"]],
                                 ["metodo_supplier_code_imp_c" => $item["CodiceMetodo"]],
                                 ["metodo_client_code_mekit_c" => $item["CodiceMetodo"]],
                                 ["metodo_supplier_code_mekit_c" => $item["CodiceMetodo"]],
                             ],
                ]
            ],
            "max_num" => 1,
            "offset" => 0,
            "fields" => "id,metodo_last_update_time_c",
        ];
        $result = $this->sugarCrmRest->comunicate('/Accounts/filter', 'GET', $arguments);
        if(isset($result->records) && count($result->records)) {
            $itemData = $result->records[0];
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
            $item["database"] = $database;
        }
        $this->index = $index;
    }

    protected function log($msg) {
        call_user_func($this->logger, $msg);
    }
}