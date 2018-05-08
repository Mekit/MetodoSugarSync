<?php
/**
 * Created by Adam Jakab.
 * Date: 17/12/15
 * Time: 12.41
 */

namespace Mekit\DbCache;

class ProductCache extends CacheDb {
    /** @var array */
    protected $columns = [
      'id' => ['type' => 'TEXT', 'index' => TRUE, 'unique' => TRUE],
      'crm_id' => ['type' => 'TEXT', 'index' => FALSE],
      'database_metodo' => ['type' => 'TEXT', 'index' => TRUE],
      'cat_main_id' => ['type' => 'TEXT', 'index' => TRUE],
      'cat_main_name' => ['type' => 'TEXT', 'index' => FALSE],
      'cat_sub_id' => ['type' => 'TEXT', 'index' => TRUE],
      'cat_sub_name' => ['type' => 'TEXT', 'index' => FALSE],
      'description' => ['type' => 'TEXT', 'index' => FALSE],
      'full_description' => ['type' => 'TEXT', 'index' => FALSE],
      'measurement_unit_c' => ['type' => 'TEXT', 'index' => FALSE],
      'stock_c' => ['type' => 'INTEGER', 'index' => FALSE],
      'price' => ['type' => 'TEXT', 'index' => FALSE],
      'price_lst_9997_c' => ['type' => 'TEXT', 'index' => FALSE],
      'price_lst_10000_c' => ['type' => 'TEXT', 'index' => FALSE],
      'cost' => ['type' => 'TEXT', 'index' => FALSE],
      'sold_last_120_days_c' => ['type' => 'TEXT', 'index' => FALSE],
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
                $query .= ", crm_id = NULL";
            }
            $query .= ";";
            $statement = $this->db->prepare($query);
            $statement->execute();
        }
    }
}