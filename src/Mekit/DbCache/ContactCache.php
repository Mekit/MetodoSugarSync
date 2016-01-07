<?php
/**
 * Created by Adam Jakab.
 * Date: 17/12/15
 * Time: 12.41
 */

namespace Mekit\DbCache;

class ContactCache extends CacheDb {
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

    public function invalidateAll() {
        $this->log("INVALIDATING LOCAL CACHE DATA FOR: " . $this->dataIdentifier);
        $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
        $query = "UPDATE " . $this->dataIdentifier . " SET"
                 . " metodo_last_update_time_c = '" . $oldDate->format("c") . "'"
                 . ", crm_last_update_time_c = '" . $oldDate->format("c") . "'"
                 . ";";
        $statement = $this->db->prepare($query);
        $statement->execute();
    }

    protected function setupDatabase() {
        $statement = $this->db->prepare(
            "SELECT COUNT(*) AS HASTABLE FROM sqlite_master WHERE type='table' AND name='" . $this->dataIdentifier . "'"
        );
        $statement->execute();
        $tabletest = $statement->fetchObject();
        $hasTable = $tabletest->HASTABLE == 1;
        if (!$hasTable) {
            $this->log("creating cache table($this->dataIdentifier)...");
            $sql = "CREATE TABLE " . $this->dataIdentifier . " ("
                   . "id TEXT NOT NULL"
                   . ", crm_id TEXT"
                   . ", first_name TEXT"
                   . ", last_name TEXT"
                   . ", salutation TEXT"
                   . ", description TEXT"
                   . ", email TEXT"
                   . ", phone_mobile TEXT"
                   . ", phone_work TEXT"
                   . ", phone_home TEXT"
                   . ", phone_fax TEXT"
                   . ", title TEXT"/* Posizione*/
                   . ", crm_export_flag_c TEXT"
                   . ", metodo_last_update_time_c TEXT NOT NULL"
                   . ", crm_last_update_time_c TEXT NOT NULL"
                   . ")";
            $this->db->exec($sql);
            $this->db->exec("CREATE UNIQUE INDEX ID ON " . $this->dataIdentifier . " (id ASC)");
//            $this->db->exec("CREATE INDEX CRMID ON " . $this->dataIdentifier . " (crm_id ASC)");
            $this->db->exec("CREATE INDEX FN ON " . $this->dataIdentifier . " (first_name ASC)");
            $this->db->exec("CREATE INDEX LN ON " . $this->dataIdentifier . " (last_name ASC)");
            $this->db->exec("CREATE INDEX EM ON " . $this->dataIdentifier . " (email ASC)");
            $this->db->exec("CREATE INDEX MOB ON " . $this->dataIdentifier . " (phone_mobile ASC)");
        }
    }
}