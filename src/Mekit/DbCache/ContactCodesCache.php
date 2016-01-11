<?php
/**
 * Created by Adam Jakab.
 * Date: 17/12/15
 * Time: 12.41
 */

namespace Mekit\DbCache;

class ContactCodesCache extends CacheDb {
    /** @var array */
    protected $columns = [
        'id' => ['type' => 'TEXT', 'index' => TRUE, 'unique' => TRUE],
        'contact_id' => ['type' => 'TEXT', 'index' => TRUE],
        'database' => ['type' => 'TEXT', 'index' => TRUE],
        'metodo_contact_code' => ['type' => 'TEXT', 'index' => TRUE],
        'metodo_code_company' => ['type' => 'TEXT', 'index' => FALSE],
        'metodo_code_cf' => ['type' => 'TEXT', 'index' => TRUE],
        'metodo_role' => ['type' => 'TEXT', 'index' => FALSE]
    ];

    /**
     * @param string   $dataIdentifier
     * @param callable $logger
     */
    public function __construct($dataIdentifier, $logger) {
        parent::__construct($dataIdentifier, $logger);
    }

    public function removeAll() {
        $tableName = $this->dataIdentifier;
        $this->log(__CLASS__ . " - REMOVING ALL CACHE DATA FOR: " . $tableName);
        $query = "DELETE FROM " . $tableName . ";";
        $statement = $this->db->prepare($query);
        $statement->execute();
    }

    public function invalidateAll() {
        //nothing to invalidate in this cache table
    }
}