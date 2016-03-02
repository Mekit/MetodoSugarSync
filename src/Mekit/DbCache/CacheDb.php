<?php
namespace Mekit\DbCache;

use Mekit\Console\Configuration;


class CacheDb extends SqliteDb {
    /** @var \PDOStatement */
    private $itemWalker = NULL;

    /** @var array */
    protected $columns = [];


    /**
     * @param string $dataIdentifier
     * @param callable $logger
     */
    public function __construct($dataIdentifier, $logger) {
        parent::__construct($dataIdentifier, $logger);
        $this->setupDatabase();
    }

    public function resetItemWalker() {
        $this->itemWalker = NULL;
    }

    public function getNextItem($orderByColumn = 'metodo_last_update_time_c', $orderDir = 'ASC') {
        $answer = FALSE;
        if (!$this->itemWalker) {
            $query = "SELECT * FROM " . $this->dataIdentifier . " ORDER BY ${orderByColumn} ${orderDir}";
            $this->itemWalker = $this->db->prepare($query);
            $this->itemWalker->execute();
        }
        try {
            $answer = $this->itemWalker->fetch(\PDO::FETCH_OBJ);
        } catch(\PDOException $e) {
            $this->log(__CLASS__ . " - load item error: " . $e->getMessage());
        }
        return $answer;
    }

    /**
     * @param array $filter
     * @param array $order
     * @return bool|array
     */
    public function loadItems($filter, $order = NULL) {
        $answer = FALSE;
        try {
            if (count($filter)) {
                $query = "SELECT * FROM " . $this->dataIdentifier;
                $parameters = [];

                //FILTERS
                if (count($filter)) {
                    $query .= " WHERE";
                    $filterIndex = 1;
                    $maxFilters = count($filter);
                    foreach ($filter as $columnName => $columnValue) {
                        $paramName = ':' . $columnName;
                        $query .= ' ' . $columnName . ' = ' . $paramName . ($filterIndex < $maxFilters ? " AND" : "");
                        $parameters[$paramName] = $columnValue;
                        $filterIndex++;
                    }
                }

                //ORDER
                if (count($order)) {
                    $query .= " ORDER BY";
                    $orderIndex = 1;
                    $maxOrder = count($order);
                    foreach ($order as $columnName => $columnDirection) {
                        $columnDirection = strtoupper(
                            (!in_array(
                                strtoupper($columnDirection), [
                                'ASC',
                                'DESC'
                            ]
                            ) ? 'ASC' : $columnDirection)
                        );
                        $query .= ' ' . $columnName . ' ' . $columnDirection . ($orderIndex < $maxOrder ? "," : "");
                    }
                }

                $stmt = $this->db->prepare($query);
                if ($stmt->execute($parameters)) {
                    $answer = $stmt->fetchAll(\PDO::FETCH_OBJ);
                }
            }
        } catch(\PDOException $e) {
            $this->log(__CLASS__ . " - load item error: " . $e->getMessage());
            if (isset($query)) {
                $this->log(__CLASS__ . " - SQL: " . $query);
            }
            if (isset($parameters)) {
                $this->log(__CLASS__ . " - PARAMETERS: " . json_encode($parameters));
            }
        }
        return $answer;
    }

    /**
     * @param \stdClass $item
     * @return bool
     */
    public function addItem($item) {
        $answer = FALSE;
        try {
            $item = $this->cleanUpItem($item);
            $columns = array_keys(get_object_vars($item));
            $query = "INSERT INTO " . $this->dataIdentifier . " "
                     . "(" . implode(",", $columns) . ")"
                     . " VALUES "
                     . "(";
            $columnIndex = 1;
            $maxColumns = count($columns);
            $parameters = [];
            foreach (get_object_vars($item) as $columnName => $columnValue) {
                $paramName = ':' . $columnName;
                $query .= $paramName . ($columnIndex < $maxColumns ? ", " : "");
                $parameters[$paramName] = $columnValue;
                $columnIndex++;
            }
            $query .= ")";
            $stmt = $this->db->prepare($query);
            $answer = $stmt->execute($parameters);
        } catch(\PDOException $e) {
            $this->log(__CLASS__ . " - item insert error: " . $e->getMessage());
            if (isset($query)) {
                $this->log(__CLASS__ . " - SQL: " . $query);
            }
            if (isset($parameters)) {
                $this->log(__CLASS__ . " - PARAMETERS: " . json_encode($parameters));
            }
        }
        return $answer;
    }

    /**
     * @param \stdClass $item
     * @return bool
     */
    public function updateItem($item) {
        $answer = FALSE;
        try {
            $item = $this->cleanUpItem($item);
            $itemId = $item->id;
            unset($item->id);
            $columns = array_keys(get_object_vars($item));

            $query = "UPDATE " . $this->dataIdentifier . " SET ";
            $columnIndex = 1;
            $maxColumns = count($columns);
            $parameters = [];
            foreach (get_object_vars($item) as $columnName => $columnValue) {
                $paramName = ':' . $columnName;
                $query .= $columnName . " = " . $paramName . ($columnIndex < $maxColumns ? ", " : "");
                $parameters[$paramName] = $columnValue;
                $columnIndex++;
            }
            //ID
            $query .= " WHERE id = :id";
            $parameters[":id"] = $itemId;
            //
            $stmt = $this->db->prepare($query);
            $answer = $stmt->execute($parameters);
        } catch(\PDOException $e) {
            $this->log(__CLASS__ . " - item update error: " . $e->getMessage());
            if (isset($query)) {
                $this->log(__CLASS__ . " - SQL: " . $query);
            }
            if (isset($parameters)) {
                $this->log(__CLASS__ . " - PARAMETERS: " . json_encode($parameters));
            }
        }
        return $answer;
    }

    /**
     * remove from item any property not defined in columns
     * @param \stdClass $item
     * @return \stdClass
     */
    protected function cleanUpItem($item) {
        $answer = clone($item);
        foreach (array_keys(get_object_vars($answer)) as $colName) {
            if (!array_key_exists($colName, $this->columns)) {
                unset($answer->$colName);
            }
        }
        return $answer;
    }

    public function removeAll() {
        $this->log(__CLASS__ . " - REMOVING ALL CACHE DATA FOR: " . $this->dataIdentifier);
        $query = "DELETE FROM " . $this->dataIdentifier . ";";
        $statement = $this->db->prepare($query);
        $statement->execute();
    }


    public function invalidateAll() {
        throw new \Exception(__CLASS__ . ": Method invalidateAll must be implemented in extending class!");
    }

    /**
     * Set up database table from $columns array
     */
    protected function setupDatabase() {
        $tableName = $this->dataIdentifier;
        $statement = $this->db->prepare(
            "SELECT COUNT(*) AS HASTABLE FROM sqlite_master WHERE type='table' AND name='" . $tableName . "'"
        );
        $statement->execute();
        $tabletest = $statement->fetchObject();
        $hasTable = $tabletest->HASTABLE == 1;
        if (!$hasTable) {
            //CREATE THE TABLE
            $sql = "CREATE TABLE " . $tableName . " (";
            $colIndex = 0;
            foreach ($this->columns as $colName => $colData) {
                $sql .= $colIndex != 0 ? ", " : "";
                $sql .= $colName . " " . $colData["type"];
                $colIndex++;
            }
            $sql .= ")";
            $this->log("creating cache table($tableName): " . $sql);
            $this->db->exec($sql);
            //CREATE INDICES
            foreach ($this->columns as $colName => $colData) {
                if (isset($colData["index"]) && $colData["index"]) {
                    $indexType = isset($colData["unique"]) && $colData["unique"] ? "UNIQUE INDEX" : "INDEX";
                    $indexName = "I_" . md5($colName . microtime(TRUE));
                    $sql = "CREATE $indexType $indexName ON $tableName ($colName ASC)";
                    $this->db->exec($sql);
                }
            }
        }
    }
}
