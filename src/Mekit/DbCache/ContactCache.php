<?php
/**
 * Created by Adam Jakab.
 * Date: 17/12/15
 * Time: 12.41
 */

namespace Mekit\DbCache;

class ContactCache extends CacheDb {
    /** @var array */
    protected $columns = [
        'id' => ['type' => 'TEXT', 'index' => TRUE, 'unique' => TRUE],
        'crm_id' => ['type' => 'TEXT', 'index' => FALSE],
        'first_name' => ['type' => 'TEXT', 'index' => TRUE],
        'last_name' => ['type' => 'TEXT', 'index' => TRUE],
        'salutation' => ['type' => 'TEXT', 'index' => FALSE],
        'description' => ['type' => 'TEXT', 'index' => FALSE],
        'email1' => ['type' => 'TEXT', 'index' => TRUE],
        'phone_mobile' => ['type' => 'TEXT', 'index' => TRUE],
        'phone_work' => ['type' => 'TEXT', 'index' => FALSE],
        'phone_home' => ['type' => 'TEXT', 'index' => FALSE],
        'phone_fax' => ['type' => 'TEXT', 'index' => FALSE],
        'crm_export_flag_c' => ['type' => 'TEXT', 'index' => FALSE],
        'metodo_last_update_time_c' => ['type' => 'TEXT', 'index' => FALSE],
        'crm_last_update_time_c' => ['type' => 'TEXT', 'index' => FALSE]
    ];

    /**
     * @param string   $dataIdentifier
     * @param callable $logger
     */
    public function __construct($dataIdentifier, $logger) {
        parent::__construct($dataIdentifier, $logger);
    }


    /**
     * @param array $filter
     * @return bool|array
     */
    public function loadItems($filter) {
        $answer = FALSE;
        /* These fields contain json encoded multiple elements*/
        $multipleDataFields = [];
        try {
            if (count($filter)) {
                $query = "SELECT * FROM " . $this->dataIdentifier . " WHERE";
                $filterIndex = 1;
                $maxFilters = count($filter);
                $parameters = [];
                foreach ($filter as $columnName => $columnValue) {
                    $paramName = ':' . $columnName;
                    $operator = '=';
                    if (in_array($columnName, $multipleDataFields)) {
                        $operator = 'LIKE';
                        $columnValue = '%"' . $columnValue . '"%';
                    }
                    $query .= ' ' . $columnName . ' ' . $operator . ' ' . $paramName . ($filterIndex
                                                                                        < $maxFilters ? " AND" : "");
                    $parameters[$paramName] = $columnValue;
                    $filterIndex++;
                }
                //$this->log("Query: " . $query . " - Params: " . json_encode($parameters));
                $stmt = $this->db->prepare($query);

                if ($stmt->execute($parameters)) {
                    $answer = $stmt->fetchAll(\PDO::FETCH_OBJ);
                }
            }
        } catch(\PDOException $e) {
            $this->log(__CLASS__ . " - load item error: " . $e->getMessage());
        }
        return $answer;
    }

    public function loadItemsOld($filter) {
        $answer = FALSE;
        try {
            if (count($filter)) {
                $query = "SELECT * FROM " . $this->dataIdentifier . " WHERE";
                $filterIndex = 1;
                $maxFilters = count($filter);
                $parameters = [];
                foreach ($filter as $columnName => $columnValue) {
                    $paramName = ':' . $columnName;
                    $operator = '=';
                    if (in_array($columnName, ["phone_mobile", "email"])) {
                        $operator = 'LIKE';
                        $columnValue = '%"' . $columnValue . '"%';
                    }
                    $query .= ' ' . $columnName . ' ' . $operator . ' ' . $paramName . ($filterIndex
                                                                                        < $maxFilters ? " AND" : "");
                    $parameters[$paramName] = $columnValue;
                    $filterIndex++;
                }
                $this->log("Query: " . $query . " - Params: " . json_encode($parameters));
                $stmt = $this->db->prepare($query);

                if ($stmt->execute($parameters)) {
                    $answer = $stmt->fetchAll(\PDO::FETCH_OBJ);
                }
            }
        } catch(\PDOException $e) {
            $this->log(__CLASS__ . " - load item error: " . $e->getMessage());
        }
        return $answer;
    }



    public function removeAll() {
        $tableName = $this->dataIdentifier;
        $this->log(__CLASS__ . " - REMOVING ALL CACHE DATA FOR: " . $tableName);
        $query = "DELETE FROM " . $tableName . ";";
        $statement = $this->db->prepare($query);
        $statement->execute();
    }

    /**
     * @param bool $local
     * @param bool $remote
     */
    public function invalidateAll($local = FALSE, $remote = FALSE) {
        if ($local || $remote) {
            $tableName = $this->dataIdentifier;
            $this->log(
                "INVALIDATING CACHE[" . (($local ? "LOCAL" : "") . "|" . ($remote ? "REMOTE" : "")) . "] DATA FOR: "
                . $tableName
            );

            $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
            $query = "UPDATE " . $tableName . " SET";

            if ($local) {
                $query .= " metodo_last_update_time_c = '" . $oldDate->format("c") . "'";
            }
            $query .= ($local && $remote) ? ',' : '';
            if ($remote) {
                $query .= " crm_last_update_time_c = '" . $oldDate->format("c") . "'";
            }

            //tmp - ONLY MEKIT - @todo: remove me!
            if (TRUE && $remote) {
                //$query .= " WHERE metodo_client_code_mekit_c IS NOT NULL";
                //$query .= " WHERE id = '47cb57a10a837f35c5a0c1b3d90341fb'";
                //
            }

            $query .= ";";


            $statement = $this->db->prepare($query);
            $statement->execute();
        }
    }
}