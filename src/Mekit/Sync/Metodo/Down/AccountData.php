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
        $maxRun = 1;
        foreach($this->index as $indexItem) {
            if($i++ >= $maxRun) {
                $this->log("Reached hard limit($maxRun).");
                break;
            }
            $codice_metodo = $indexItem["codice_metodo"];
            $this->log("elab item with codice_metodo: '" . $codice_metodo . "'");
            $localItem = $this->loadLocalItem($codice_metodo);
            //print_r($localItem);
            $remoteItem = $this->loadRemoteItem($codice_metodo);
            //print_r($remoteItem);
            $syncItem = $this->createSyncItem($localItem, $remoteItem);
            //print_r($syncItem);
            //$res = $this->saveRemoteItem($syncItem);
            //print_r($res);
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
     */
    protected function createSyncItem($localItem, $remoteItem) {
        $syncItem = false;
        if($localItem) {
            $syncItem = [];
            if($remoteItem) {
                $syncItem["id"] = $remoteItem->id;
                //@todo: map other stuff?
            }
            $syncItem["name"] = $localItem->RagioneSociale;
            $syncItem["sync_codes_c"] = $localItem->CodiceMetodo;
            $syncItem["codice_fiscale_c"] = $localItem->CodiceFiscale;
            $syncItem["partita_iva_c"] = $localItem->PartitaIVA;
        }
        return $syncItem;
    }

    /**
     * @param string $codice_metodo
     * @return \stdClass|bool
     */
    protected function loadRemoteItem($codice_metodo) {
        $itemData = false;
        $arguments = [
            "filter" => [
                [
                    "sync_codes_c" => $codice_metodo,
                ],
            ],
            "max_num" => 1,
            "offset" => 0,
            "fields" => "id",
        ];
        $result = $this->sugarCrmRest->comunicate('/Accounts/filter', 'GET', $arguments);
        if(isset($result->records) && count($result->records)) {
            $itemData = $result->records[0];
            //$itemData = $this->sugarCrmRest->comunicate('/Accounts/' . $id, 'GET');
        }

        return($itemData);
    }

    /**
     * @param string $codice_metodo
     * @return \stdClass
     */
    protected function loadLocalItem($codice_metodo) {
        $db = Configuration::getDatabaseConnection("SERVER2K8");
        $query = "SELECT * FROM [IMP].[dbo].[SogCRM_AnagraficaCF] WHERE [CodiceMetodo] = '" . $codice_metodo . "'";
        $statement = $db->prepare($query);
        $statement->execute();
        $itemData = $statement->fetch(\PDO::FETCH_OBJ);
        return($itemData);
    }

    protected function loadIndex() {
        $db = Configuration::getDatabaseConnection("SERVER2K8");
        $query = "SELECT [CodiceMetodo] AS codice_metodo FROM [IMP].[dbo].[SogCRM_AnagraficaCF]";
        $statement = $db->prepare($query);
        $statement->execute();
        $this->index = $statement->fetchAll(\PDO::FETCH_ASSOC);
        $this->log("number of elements: " . count($this->index));
    }

    protected function log($msg) {
        call_user_func($this->logger, $msg);
    }
}