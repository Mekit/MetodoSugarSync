<?php
/**
 * Created by Adam Jakab.
 * Date: 11/02/16
 * Time: 11.59
 */

namespace Mekit\Sync\CrmToMetodo;

use Mekit\Console\Configuration;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRest;
use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;


class AccountData extends Sync implements SyncInterface {
    /** @var callable */
    protected $logger;

    /** @var SugarCrmRest */
    protected $sugarCrmRest;

    /** @var array */
    protected $counters = [];

    /** @var array */
    protected $flagCodeColumns = [
        'imp_sync_as_client_c' => [
            'codeColumn' => 'imp_metodo_client_code_c',
            'database' => 'IMP',
            'prefix' => 'C'
        ],
        'imp_sync_as_supplier_c' => [
            'codeColumn' => 'imp_metodo_supplier_code_c',
            'database' => 'IMP',
            'prefix' => 'F'
        ],
        'mekit_sync_as_client_c' => [
            'codeColumn' => 'mekit_metodo_client_code_c',
            'database' => 'MEKIT',
            'prefix' => 'C'
        ],
        'mekit_sync_as_supplier_c' => [
            'codeColumn' => 'mekit_metodo_supplier_code_c',
            'database' => 'MEKIT',
            'prefix' => 'F'
        ]
    ];

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
     */
    public function execute($options) {
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
                $this->log("OPERATION(AFTER EXECUTION): " . json_encode($operation));
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
                    $codeColumnName = $operation['codeColumn'];
                    $flagColumnName = $operation['flagColumn'];
                    //reset operation flag so update does not occur again
                    $nameValueList[] = ['name' => $flagColumnName, 'value' => 0];
                    //set 'codiceMetodo' where INSERT operation was done
                    if ($operation['sqlCommand'] == 'INSERT') {
                        $nameValueList[] = ['name' => $codeColumnName, 'value' => $operation['CODCONTO']];
                    }
                }
            }
            $arguments = [
                'module_name' => 'Accounts',
                'name_value_list' => $nameValueList
            ];
            $result = $this->sugarCrmRest->comunicate('set_entry', $arguments);
            $this->log("Crm Update Result: " . json_encode($result));
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
        $operation['sql'][] = $this->getSaveSqlFor_ANAGRAFICACF($remoteItem, $operation);
        $operation['sql'][] = $this->getSaveSqlFor_ANAGRAFICARISERVATICF($remoteItem, $operation);
        $operation['sql'][] = $this->getSaveSqlFor_EXTRA_CLIENTI_FORNITORI($remoteItem, $operation);
        $operation['sql'][] = $this->getSaveSqlFor_TP_EXTRA_CLIENTI_FORNITORI($remoteItem, $operation);
        foreach ($operation['sql'] as $sql) {
            try {
                $res = $this->executeSqlOnLocalDb($sql);
            } catch(\Exception $e) {
                $this->log("SQL EXEC FAILURE: " . $e->getMessage());
                $this->log("FAILED SQL: " . $sql);
                $res = FALSE;
            }
            $operation['success'] = $operation['success'] && $res;
        }
        return $operation;
    }

    /**
     * @param \stdClass $remoteItem
     * @param array     $operation
     * @return string
     */
    protected function getSaveSqlFor_TP_EXTRA_CLIENTI_FORNITORI($remoteItem, $operation) {
        $tableName = $operation['prefix'] == 'C' ? 'TP_EXTRACLIENTI' : 'TP_EXTRAFORNITORI';
        $now = new \DateTime();
        $tableData = [
            'CODCONTO' => $operation['CODCONTO'],
            //
            'UTENTEMODIFICA' => $this->METODO_UTENTEMODIFICA,
            'DATAMODIFICA' => $now->format("Y-m-d H:i:s"),
        ];

        if ($operation['sqlCommand'] == 'INSERT') {
            $answer = $this->getInsertUpdateSql(
                'INSERT',
                $operation['database'], $tableName, $tableData
            );
        }
        else {
            $answer = "PRINT 'NO SYNC UPDATE OPERATION IS NECESSARY FOR TABLE: " . $tableName . "';";
        }
        return $answer;
    }

    /**
     * @param \stdClass $remoteItem
     * @param array     $operation
     * @return string
     */
    protected function getSaveSqlFor_EXTRA_CLIENTI_FORNITORI($remoteItem, $operation) {
        $tableName = $operation['prefix'] == 'C' ? 'EXTRACLIENTI' : 'EXTRAFORNITORI';
        $now = new \DateTime();
        $tableData = [
            'CODCONTO' => $operation['CODCONTO'],
            'UTENTEMODIFICA' => $this->METODO_UTENTEMODIFICA,
            'DATAMODIFICA' => $now->format("Y-m-d H:i:s"),
        ];

        if ($tableName == 'EXTRACLIENTI') {
            //Only IMP has this field - it needs to be set to empty string otherwise it complains about invalid value
            if ($operation['database'] == 'IMP') {
                $tableData = array_merge(
                    $tableData, [
                                  'CodiceAteco' => ''
                              ]
                );
            }
        }

        if ($operation['sqlCommand'] == 'INSERT') {
            $answer = $this->getInsertUpdateSql(
                'INSERT',
                $operation['database'], $tableName, $tableData
            );
        }
        else {
            $answer = "PRINT 'NO SYNC UPDATE OPERATION IS NECESSARY FOR TABLE: " . $tableName . "';";
        }
        return $answer;
    }

    /**
     * @param \stdClass $remoteItem
     * @param array     $operation
     * @return string
     */
    protected function getSaveSqlFor_ANAGRAFICARISERVATICF($remoteItem, $operation) {
        $tableName = 'ANAGRAFICARISERVATICF';
        $now = new \DateTime();

        $tableData = [
            'CODCONTO' => $operation['CODCONTO'],
            'ESERCIZIO' => $now->format("Y"),
            'CODCAMBIO' => '1',
            'CODZONA' => $remoteItem->zone_c,
            //
            'UTENTEMODIFICA' => $this->METODO_UTENTEMODIFICA,
            'DATAMODIFICA' => $now->format("Y-m-d H:i:s"),
        ];

        if ($operation['database'] == "IMP") {
            $tableData['CODAGENTE1'] = $this->fixMetodoCode($remoteItem->imp_agent_code_c, ['A']);
            $tableData['CODSETTORE'] = $remoteItem->industry;
        }
        if ($operation['database'] == "MEKIT") {
            $tableData['CODAGENTE1'] = $this->fixMetodoCode($remoteItem->mekit_agent_code_c, ['A']);
            $tableData['CODSETTORE'] = $remoteItem->mekit_industry_c;
        }

        if ($operation['sqlCommand'] == 'INSERT') {
            /*
             * Su CRM creiamo aziende dove il cliente di fatturazione('CODCONTOFATT') corrisponde SEMPRE
             * al codice cliente $operation['CODCONTO']
             * We set this data only when creating
             *
             */
            $tableData['CODCONTOFATT'] = $tableData['CODCONTO'];

            $answer = $this->getInsertUpdateSql(
                'INSERT',
                $operation['database'], $tableName, $tableData
            );
        }
        else {
            unset($tableData['CODCONTO']);
            unset($tableData['ESERCIZIO']);
            unset($tableData['CODCAMBIO']);
            $answer = $this->getInsertUpdateSql(
                'UPDATE',
                $operation['database'], $tableName, $tableData,
                [
                    'CODCONTO' => $operation['CODCONTO'],
                    'ESERCIZIO' => $now->format("Y")
                ]
            );
        }
        return $answer;
    }

    /**
     * @param \stdClass $remoteItem
     * @param array     $operation
     * @return string
     */
    protected function getSaveSqlFor_ANAGRAFICACF($remoteItem, $operation) {
        $tableName = 'ANAGRAFICACF';
        $now = new \DateTime();

        $primaryEmailAddress = '';
        if (isset($remoteItem->email_addresses) && is_array($remoteItem->email_addresses)
            && count($remoteItem->email_addresses)
        ) {
            /** @var \stdClass $mailAddressObj */
            foreach ($remoteItem->email_addresses as $mailAddressObj) {
                if ($mailAddressObj->primary_address) {
                    //looks like it is not set by crm
                    $primaryEmailAddress = $mailAddressObj->email_address;
                    break;
                }
            }
            if (empty($primaryEmailAddress)) {
                $primaryEmailAddress = $remoteItem->email_addresses[0]->email_address;
            }
        }

        $tableData = [
            'TIPOCONTO' => $operation['prefix'],
            'CODCONTO' => $operation['CODCONTO'],
            'DSCCONTO1' => substr($remoteItem->name, 0, 80),//this is max size in Metodo
            'INDIRIZZO' => $remoteItem->billing_address_street,
            'CAP' => $remoteItem->billing_address_postalcode,
            'LOCALITA' => $remoteItem->billing_address_city,
            'PROVINCIA' => substr($remoteItem->billing_address_state, 0, 2),
            //@todo: temorary solution - we need to fix db!
            'TELEFONO' => $remoteItem->phone_office,
            'FAX' => $remoteItem->phone_fax,
            'TELEX' => $primaryEmailAddress,
            'CODFISCALE' => $remoteItem->codice_fiscale_c,
            'PARTITAIVA' => $remoteItem->vat_number_c,
            'NOTE' => '',
            'INDIRIZZOINTERNET' => $remoteItem->website,
            //PREDEFINITI
            'CODMASTRO' => ($operation['prefix'] == 'C' ? '1070' : '2050'),
            'CODNAZIONE' => 0,
            'CODICEISO' => 'IT',
            'CODLINGUA' => 0,
            //
            'UTENTEMODIFICA' => $this->METODO_UTENTEMODIFICA,
            'DATAMODIFICA' => $now->format("Y-m-d H:i:s"),
        ];

        if ($operation['sqlCommand'] == 'INSERT') {
            $tableData['DSCCONTO2'] = 'ANAGRAFICA INCOMPLETA';
        }

        if ($operation['sqlCommand'] == 'INSERT') {
            $answer = $this->getInsertUpdateSql(
                'INSERT',
                $operation['database'], $tableName, $tableData
            );
        }
        else {
            unset($tableData['TIPOCONTO']);
            unset($tableData['CODCONTO']);
            unset($tableData['CODMASTRO']);
            unset($tableData['CODNAZIONE']);
            unset($tableData['CODICEISO']);
            unset($tableData['CODLINGUA']);
            $answer = $this->getInsertUpdateSql(
                'UPDATE',
                $operation['database'], $tableName, $tableData,
                ['CODCONTO' => $operation['CODCONTO']]
            );
        }

        return $answer;
    }

    /**
     * @param string $operation
     * @param string $database
     * @param string $tableName
     * @param array  $tableData
     * @param array  $whereColumns
     * @return string
     */
    protected function getInsertUpdateSql($operation, $database, $tableName, $tableData, $whereColumns = NULL) {
        $answer = '';
        //CLEAN UP TABLE FROM EMPTY VALUES
        foreach ($tableData as $columnName => $columnValue) {
            $columnValue = $this->cleanupSqlFieldValue($columnValue);
            if (is_null($columnValue)) {
                unset($tableData[$columnName]);
            }
        }
        //
        $columnIndex = 1;
        $columnNames = array_keys($tableData);
        $maxColumns = count($columnNames);
        if ($operation == 'INSERT') {
            $answer .= 'INSERT INTO [' . $database . '].[dbo].[' . $tableName . ']';
            $answer .= " (" . implode(",", $columnNames) . ")";
            $answer .= " VALUES(";
            foreach ($tableData as $columnName => $columnValue) {
                $columnValueNorm = $this->cleanupSqlFieldValue($columnValue);
                /*
                 * This breaks 'vat id' like '012345' => 12345
                 * the check should test if strlen($data) == strlen((string)intval($data))
                if (is_numeric($columnValueNorm)) {
                    $answer .= $columnValueNorm;
                }
                else {
                    $answer .= "'" . $columnValueNorm . "'";
                }
                */
                $answer .= "'" . $columnValueNorm . "'";

                $answer .= ($columnIndex < $maxColumns ? ", " : "");
                $columnIndex++;
            }
            $answer .= ");";
        }
        else if ($operation == 'UPDATE') {
            $answer .= 'UPDATE [' . $database . '].[dbo].[' . $tableName . '] SET ';
            foreach ($tableData as $columnName => $columnValue) {
                $columnValueNorm = $this->cleanupSqlFieldValue($columnValue);
                $answer .= $columnName . " = ";
                /*
                 * ...as above...
                if (is_numeric($columnValueNorm)) {
                    $answer .= $columnValueNorm;
                }
                else {
                    $answer .= "'" . $columnValueNorm . "'";
                }
                */
                $answer .= "'" . $columnValueNorm . "'";
                $answer .= ($columnIndex < $maxColumns ? ", " : "");
                $columnIndex++;
            }
            if (($whereMaxColumns = count($whereColumns))) {
                $whereColumnIndex = 1;
                $answer .= " WHERE ";
                foreach ($whereColumns as $columnName => $columnValue) {
                    $columnValueNorm = $this->cleanupSqlFieldValue($columnValue);
                    $answer .= $columnName . " = ";
                    /*
                     * ...as above...
                    if (is_numeric($columnValueNorm)) {
                        $answer .= $columnValueNorm;
                    }
                    else {
                        $answer .= "'" . $columnValueNorm . "'";
                    }
                    */
                    $answer .= "'" . $columnValueNorm . "'";
                    $answer .= ($whereColumnIndex < $whereMaxColumns ? " AND " : "");
                    $whereColumnIndex++;
                }
            }
        }
        return $answer;
    }

    /**
     * @param mixed $value
     * @return mixed|null
     */
    protected function cleanupSqlFieldValue($value) {
        $value = str_replace("'", "`", $value);
        if (empty($value)) {
            $value = '';
        }
        return $value;
    }


    /**
     * @param \stdClass $remoteItem
     * @return array
     */
    protected function getOperationsForRemoteItem($remoteItem) {
        $answer = [];
        /**
         * @var string $flagColName
         * @var array  $operationData
         */
        foreach ($this->flagCodeColumns as $flagColName => $operationData) {
            if ($remoteItem->$flagColName == 1) {
                $codeColumnName = $operationData['codeColumn'];
                if (!empty($remoteItem->$codeColumnName)) {
                    $operationData['sqlCommand'] = 'UPDATE';
                    $operationData['CODCONTO'] = $remoteItem->$codeColumnName;
                }
                else {
                    $operationData['sqlCommand'] = 'INSERT';
                    $operationData['CODCONTO'] = $this->getNextCodiceMetodo($operationData['database'], $operationData['prefix']);
                }
                //I just add this so I do not have to search for it later in updateRemoteItemWithOperationData
                $operationData['flagColumn'] = $flagColName;
                $answer[] = $operationData;
            }
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
     * @param string $database
     * @param string $prefix - C|F
     * @return bool|string
     */
    protected function getNextCodiceMetodo($database, $prefix) {
        $answer = FALSE;
        $db = Configuration::getDatabaseConnection("SERVER2K8");
        $sql = "SELECT TOP(1) (CAST(RIGHT(ACF.CODCONTO,6) AS INT) + 1) AS NUMERIC_ID
            FROM [$database].[dbo].[ANAGRAFICACF] AS ACF
            WHERE ACF.CODCONTO LIKE '" . $prefix . "%'
            AND ACF.CODCONTO NOT LIKE '" . $prefix . "9%'
            ORDER BY NUMERIC_ID DESC;
        ";
        $statement = $db->prepare($sql);
        $statement->execute();
        $nextCodeNumber = $statement->fetch(\PDO::FETCH_COLUMN);
        if ($nextCodeNumber) {
            $maxCodeLenght = 7;
            $numericCodeLength = strlen($nextCodeNumber);
            $answer = $prefix . str_repeat(" ", $maxCodeLenght - 1 - $numericCodeLength) . $nextCodeNumber;
        }
        return $answer;
    }


    /**
     * @return \stdClass|bool
     */
    protected function getNextRemoteItem() {
        $answer = FALSE;
        $arguments = [
            'module_name' => 'Accounts',
            'query' => "accounts_cstm.imp_sync_as_client_c = 1
             OR accounts_cstm.imp_sync_as_supplier_c = 1
             OR accounts_cstm.mekit_sync_as_client_c = 1
             OR accounts_cstm.mekit_sync_as_supplier_c = 1
             ",
            'order_by' => '',
            'offset' => 0,
            'select_fields' => [
                'id',
                'imp_sync_as_client_c',
                'imp_sync_as_supplier_c',
                'mekit_sync_as_client_c',
                'mekit_sync_as_supplier_c',
                'imp_metodo_client_code_c',
                'imp_metodo_supplier_code_c',
                'mekit_metodo_client_code_c',
                'mekit_metodo_supplier_code_c',
                'name',
                'vat_number_c',
                'codice_fiscale_c',
                'phone_office',
                'phone_fax',
                'billing_address_street',
                'billing_address_postalcode',
                'billing_address_city',
                'billing_address_state',
                'billing_address_country',
                'website',
                //
                'zone_c',
                'imp_agent_code_c',
                'mekit_agent_code_c',
                'industry',
                'mekit_industry_c'
            ],
            'link_name_to_fields_array' => [
                ['name' => 'email_addresses', 'value' => ['email_address', 'opt_out', 'primary_address']]
            ],
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

    /**
     * @param string $originalCode
     * @param array  $prefixes
     * @param bool   $nospace - Do NOT space prefix from number - new crm does not have spaces in dropdowns
     * @return string
     */
    protected function fixMetodoCode($originalCode, $prefixes, $nospace = FALSE) {
        $normalizedCode = '';
        if (!empty($originalCode)) {
            $codeLength = 7;
            $normalizedCode = '';
            $PREFIX = strtoupper(substr($originalCode, 0, 1));
            $NUMBER = trim(substr($originalCode, 1));
            $SPACES = '';
            if (in_array($PREFIX, $prefixes)) {
                if (0 != (int) $NUMBER) {
                    if (!$nospace) {
                        $SPACES = str_repeat(' ', $codeLength - strlen($PREFIX) - strlen($NUMBER));
                    }
                    $normalizedCode = $PREFIX . $SPACES . $NUMBER;
                }
                else {
                    //$this->log("UNSETTING BAD CODE[not numeric]: '" . $originalCode . "'");
                }
            }
            else {
                //$this->log("UNSETTING BAD CODE[not C|F]: '" . $originalCode . "'");
            }
        }
        return $normalizedCode;
    }
}