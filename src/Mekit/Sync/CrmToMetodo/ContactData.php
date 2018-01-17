<?php
/**
 * Created by Adam Jakab.
 * Date: 12/02/16
 * Time: 16.12
 */

namespace Mekit\Sync\CrmToMetodo;

use Mekit\Console\Configuration;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRest;
use Mekit\Sync\ConversionHelper;
use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;

class ContactData extends Sync implements SyncInterface {
    /** @var callable */
    protected $logger;

    /** @var SugarCrmRest */
    protected $sugarCrmRest;

    /** @var array */
    protected $counters = [];

    /** @var  string */
    protected $METODO_UTENTEMODIFICA;

    /**
     * @param callable $logger
     */
    public function __construct($logger) {
        parent::__construct($logger);
        $this->sugarCrmRest = new SugarCrmRest();
        $cfg = Configuration::getConfiguration();
        $this->METODO_UTENTEMODIFICA = $cfg["database"]['SERVER2K8']["username"];
    }

    /**
     * @param array $options
     * @param array $arguments
     */
  public function execute($options, $arguments)
  {
        //$this->log("EXECUTING..." . json_encode($options));
        $this->updateMetodoFromCrm();
    }

    protected function updateMetodoFromCrm() {
        $this->counters["remote"]["index"] = 0;
        while ($remoteItem = $this->getNextRemoteItem()) {
            $this->counters["remote"]["index"]++;
            $this->saveRemoteItemInMetodoMain($remoteItem);
        }
    }

    /**
     * The data present on the remoteItem (imp_sync_as_client_c, imp_sync_as_supplier_c...)
     * should determin in which database and with what prefix (C|F) to save this remote item
     * Save operation should be executed for each flag set
     *
     * If the corresponding code is set  (imp_sync_as_client_c -> imp_metodo_client_code_c)
     * it means that Metodo already has this item so instead of INSERT we UPDATE
     * @param \stdClass $remoteItem
     */
    protected function saveRemoteItemInMetodoMain($remoteItem) {
        $this->log(
            "-----------------------------------------------------------------------------------------"
            . $this->counters["remote"]["index"]
        );
        $this->log("REMOTE ITEM: " . json_encode($remoteItem));
        $operations = $this->getOperationsForRemoteItem($remoteItem);
        if (count($operations)) {
            foreach ($operations as &$operation) {
                $operation = $this->saveRemoteItemInMetodo($remoteItem, $operation);
                $this->log("\n\nOPERATION: " . json_encode($operation));
            }
            //operations array has been modified with result of save execution in 'success' key
            //if true -> we need to push back the new CODES to CRM and UNSET the flags for sync
            $this->updateRemoteItemWithOperationData($remoteItem, $operations);
        }
    }

    /**
     * @param \stdClass $remoteItem
     * @param array     $operations
     */
    protected function updateRemoteItemWithOperationData($remoteItem, $operations) {
        if (count($operations)) {
            $nameValueList = [
                ['name' => 'id', 'value' => $remoteItem->id]
            ];

            foreach ($operations as $operation) {
                if ($operation['success']) {
                    //reset operation flag so update does not occur again
                    $nameValueList[] = ['name' => 'metodo_sync_up_c', 'value' => 0];
                    //set 'codiceMetodo' where INSERT operation was done
                    if ($operation['sqlCommand'] == 'INSERT') {
                        $nameValueList[] = ['name' => 'metodo_contact_code_c', 'value' => $operation['CODCONTO']];
                    }
                }
            }
            $arguments = [
                'module_name' => 'Contacts',
                'name_value_list' => $nameValueList
            ];
            $result = $this->sugarCrmRest->comunicate('set_entry', $arguments);
            $this->log("\n\nRemoteUpdateRes: " . json_encode($result));
        }
    }

    /**
     * @param \stdClass $remoteItem
     * @param array     $operation
     * @return array
     */
    protected function saveRemoteItemInMetodo($remoteItem, $operation) {
        $operation['success'] = TRUE;
        $operation['sql'] = [];
        $operation['sql'][] = $this->getSaveSqlFor_AnagraficaContatti($remoteItem, $operation);
        foreach ($operation['sql'] as $sql) {
            $res = $this->executeSqlOnLocalDb($sql);
            $operation['success'] = $operation['success'] && $res;
        }
        return $operation;
    }

    /**
     * @param \stdClass $remoteItem
     * @param array     $operationData
     * @return string
     */
    protected function getSaveSqlFor_AnagraficaContatti($remoteItem, $operationData) {
        $answer = '';
        $tableName = 'AnagraficaContatti';
        $now = new \DateTime();

        //
        $tableData = [
          'IdContatto' => $operationData['CODCONTO'],
          'Titolo' => $remoteItem->salutation,
          'Nome' => $remoteItem->first_name,
          'Cognome' => $remoteItem->last_name,
          'Cellulare' => $remoteItem->phone_mobile,
          'TelUfficio' => $remoteItem->phone_work,
          'TelCasa' => $remoteItem->phone_home,
          'Fax' => $remoteItem->phone_fax,
          'Email' => $remoteItem->email1,
          'Note' => substr($remoteItem->description, 225),
          //
          'UtenteModifica' => $this->METODO_UTENTEMODIFICA,
          'DataModifica' => $now->format("Y-m-d H:i:s"),
        ];

        $operation = $operationData['sqlCommand'];

        if ($operation == 'UPDATE') {
            unset($tableData['IdContatto']);
        }

        $columnIndex = 1;
        $columnNames = array_keys($tableData);
        $maxColumns = count($columnNames);

        //HEAD
        if ($operation == 'INSERT') {
            $answer .= 'INSERT INTO [Crm2Metodo].[dbo].[' . $tableName . ']';
            $answer .= " (" . implode(",", $columnNames) . ")";
            $answer .= " VALUES(";
        }
        else if ($operation == 'UPDATE') {
            $answer .= 'UPDATE [Crm2Metodo].[dbo].[' . $tableName . '] SET ';
        }

        //COLUMNS - DATA
        foreach ($tableData as $columnName => $columnValue) {
            $columnValueNorm = ConversionHelper::cleanupMSSQLFieldValue($columnValue);
            $columnValueNorm = ConversionHelper::hexEncodeDataForMSSQL($columnValueNorm);

            if ($operation == 'UPDATE') {
                $answer .= $columnName . " = ";
            }
            if (substr($columnValueNorm, 0, 2) == '0x') {
                $answer .= $columnValueNorm;
            }
            else {
                $answer .= "'" . $columnValueNorm . "'";
            }

            $answer .= ($columnIndex < $maxColumns ? ", " : "");
            $columnIndex++;
        }

        if ($operation == 'INSERT') {
            $answer .= ");";
        }
        else if ($operation == 'UPDATE') {
            $answer .= " WHERE IdContatto = " . "'" . $operationData['CODCONTO'] . "';";
        }
        return $answer;
    }


    /**
     * @param \stdClass $remoteItem
     * @return array
     */
    protected function getOperationsForRemoteItem($remoteItem) {
        $answer = [];
        if ($remoteItem->metodo_sync_up_c == 1) {
            $operationData = [];
            if (!empty($remoteItem->metodo_contact_code_c)) {
                $operationData['sqlCommand'] = 'UPDATE';
                $operationData['CODCONTO'] = $remoteItem->metodo_contact_code_c;
            }
            else {
                $operationData['sqlCommand'] = 'INSERT';
                $operationData['CODCONTO'] = $this->getNextCodiceMetodo();
            }
            $answer[] = $operationData;
        }
        return $answer;
    }

    /**
     * @param string $sql
     * @return bool
     */
    protected function executeSqlOnLocalDb($sql) {
        $this->log("Excecuting SQL: " . $sql);
        $db = Configuration::getDatabaseConnection("SERVER2K8");
        $statement = $db->prepare($sql);
        $answer = $statement->execute();
        return $answer;
    }


    /**
     * @return bool|string
     */
    protected function getNextCodiceMetodo() {
        $answer = FALSE;
        $db = Configuration::getDatabaseConnection("SERVER2K8");
        $sql = "SELECT TOP(1) (CAST(AC.IdContatto AS INT) + 1) AS NUMERIC_ID
            FROM [Crm2Metodo].[dbo].[AnagraficaContatti] AS AC
            ORDER BY NUMERIC_ID DESC;
        ";
        $statement = $db->prepare($sql);
        $statement->execute();
        $nextCodeNumber = $statement->fetch(\PDO::FETCH_COLUMN);
        if ($nextCodeNumber) {
            $answer = $nextCodeNumber;
        }
        return $answer;
    }

    /**
     * @return \stdClass|bool
     */
    protected function getNextRemoteItem() {
        $answer = FALSE;
        $arguments = [
            'module_name' => 'Contacts',
            'query' => "contacts_cstm.metodo_sync_up_c = 1",
            'order_by' => '',
            'offset' => 0,
            'select_fields' => [
                'id',
                'metodo_sync_up_c',
                'metodo_contact_code_c',
                'first_name',
                'last_name',
                'salutation',
                'phone_mobile',
                'phone_work',
                'phone_home',
                'phone_fax',
                'email1',
                'description'
            ],
            'link_name_to_fields_array' => [],
            'max_results' => 1,
            'deleted' => FALSE,
            'Favorites' => FALSE,
        ];
        /** @var \stdClass $result */
        $result = $this->sugarCrmRest->comunicate('get_entry_list', $arguments);
        $entryListItem = (isset($result->entry_list) && isset($result->entry_list[0])) ? $result->entry_list[0] : NULL;
        $relationshipListItem = (isset($result->relationship_list)
                                 && isset($result->relationship_list[0])) ? $result->relationship_list[0] : NULL;
        if ($entryListItem) {
            $answer = $this->sugarCrmRest->getNameValueListFromEntyListItem($entryListItem, $relationshipListItem);
        }
        return $answer;
    }
}