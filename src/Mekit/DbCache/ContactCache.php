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
        'email' => ['type' => 'TEXT', 'index' => TRUE],
        'phone_mobile' => ['type' => 'TEXT', 'index' => TRUE],
        'phone_work' => ['type' => 'TEXT', 'index' => FALSE],
        'phone_home' => ['type' => 'TEXT', 'index' => FALSE],
        'phone_fax' => ['type' => 'TEXT', 'index' => FALSE],
        'title' => ['type' => 'TEXT', 'index' => FALSE],
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

    public function removeAll() {
        $tableName = $this->dataIdentifier;
        $this->log(__CLASS__ . " - REMOVING ALL CACHE DATA FOR: " . $tableName);
        $query = "DELETE FROM " . $tableName . ";";
        $statement = $this->db->prepare($query);
        $statement->execute();
    }

    public function invalidateAll() {
        $tableName = $this->dataIdentifier;
        $this->log("INVALIDATING LOCAL CACHE DATA FOR: " . $tableName);
        $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
        $query = "UPDATE " . $tableName . " SET"
                 . " metodo_last_update_time_c = '" . $oldDate->format("c") . "'"
                 . ", crm_last_update_time_c = '" . $oldDate->format("c") . "'"
                 . ";";
        $statement = $this->db->prepare($query);
        $statement->execute();
    }


}