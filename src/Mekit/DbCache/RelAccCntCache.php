<?php
/**
 * Created by Adam Jakab.
 * Date: 17/12/15
 * Time: 12.41
 */

namespace Mekit\DbCache;

class RelAccCntCache extends CacheDb {
    /** @var array */
    protected $columns = [
        'id' => ['type' => 'TEXT', 'index' => TRUE, 'unique' => TRUE],
        'metodo_contact_id' => ['type' => 'TEXT', 'index' => TRUE],
        'metodo_cf_id' => ['type' => 'TEXT', 'index' => TRUE],
        'metodo_role_id' => ['type' => 'TEXT', 'index' => TRUE],
        'rel_table' => ['type' => 'TEXT', 'index' => TRUE],
        'crm_account_id' => ['type' => 'TEXT', 'index' => TRUE],
        'crm_contact_id' => ['type' => 'TEXT', 'index' => TRUE],
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
            $query .= ";";

            $statement = $this->db->prepare($query);
            $statement->execute();
        }
    }
}