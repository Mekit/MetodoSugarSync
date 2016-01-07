<?php
namespace Mekit\DbCache;

use Mekit\Console\Configuration;


class CacheDb extends SqliteDb {
    /** @var \PDOStatement */
    private $itemWalker = NULL;


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

    public function getNextItem() {
        $answer = FALSE;
        if (!$this->itemWalker) {
            $query = "SELECT * FROM " . $this->dataIdentifier . " ORDER BY metodo_last_update_time_c ASC";
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
                    $query .= ' ' . $columnName . ' = ' . $paramName . ($filterIndex < $maxFilters ? " AND" : "");
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

    /**
     * @param $item
     * @return bool
     */
    public function addItem($item) {
        $answer = FALSE;
        try {
            $columns = array_keys(get_object_vars($item));
            $query = "INSERT INTO " . $this->dataIdentifier . " "
                     . "(" . implode(",", $columns) . ")"
                     . " VALUES "
                     . "(";
            $columnIndex = 1;
            $maxColumns = count($columns);
            foreach ($columns as $column) {
                $query .= ":" . $column . ($columnIndex < $maxColumns ? ", " : "");
                $columnIndex++;
            }
            $query .= ")";

            $stmt = $this->db->prepare($query);
            foreach ($columns as $column) {
                if (isset($item->$column)) {
                    $stmt->bindParam(':' . $column, $item->$column);
                }
            }
            $stmt->execute();
            $answer = TRUE;
        } catch(\PDOException $e) {
            $this->log(__CLASS__ . " - add item error: " . $e->getMessage());
        }
        return $answer;
    }

    /**
     * @param $item
     * @return bool
     */
    public function updateItem($item) {
        $answer = FALSE;
        try {
            $itemId = $item->id;
            unset($item->id);
            $columns = array_keys(get_object_vars($item));

            $query = "UPDATE " . $this->dataIdentifier . " SET ";
            $columnIndex = 1;
            $maxColumns = count($columns);
            foreach ($columns as $column) {
                $query .= $column . " = :" . $column . ($columnIndex < $maxColumns ? ", " : "");
                $columnIndex++;
            }
            $query .= " WHERE id = :id";
            $stmt = $this->db->prepare($query);
            foreach ($columns as $column) {
                if (isset($item->$column)) {
                    $stmt->bindParam(':' . $column, $item->$column);
                }
            }
            $stmt->bindParam(':id', $itemId);
            $answer = $stmt->execute();
            if (!$answer) {
                $this->log("NOT UPDATED!");
            }
        } catch(\PDOException $e) {
            $this->log(__CLASS__ . " - update item error: " . $e->getMessage());
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
     * @throws \Exception
     */
    protected function setupDatabase() {
        throw new \Exception(__CLASS__ . ": Method setupDatabase must be implemented in extending class!");
    }
}
