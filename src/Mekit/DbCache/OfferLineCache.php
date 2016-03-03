<?php
/**
 * Created by Adam Jakab.
 * Date: 17/12/15
 * Time: 12.41
 */

namespace Mekit\DbCache;

class OfferLineCache extends CacheDb {
    /** @var array */
    protected $columns = [
        'id' => ['type' => 'TEXT', 'index' => TRUE, 'unique' => TRUE],
        'crm_id' => ['type' => 'TEXT', 'index' => FALSE],
        'database_metodo' => ['type' => 'TEXT', 'index' => TRUE],
        'offer_id' => ['type' => 'TEXT', 'index' => TRUE],
        'id_line' => ['type' => 'TEXT', 'index' => TRUE],
        'line_order' => ['type' => 'INTEGER', 'index' => FALSE],
        'article_code' => ['type' => 'TEXT', 'index' => FALSE],
        'article_description' => ['type' => 'TEXT', 'index' => FALSE],
        'price_list_number' => ['type' => 'TEXT', 'index' => FALSE],
        'quantity' => ['type' => 'TEXT', 'index' => FALSE],
        'measure_unit' => ['type' => 'TEXT', 'index' => FALSE],
        'net_total' => ['type' => 'TEXT', 'index' => FALSE],
        'net_unit' => ['type' => 'TEXT', 'index' => FALSE],
        'net_total_listino_42' => ['type' => 'TEXT', 'index' => FALSE],
        'metodo_last_update_time' => ['type' => 'TEXT', 'index' => FALSE],
        'crm_last_update_time' => ['type' => 'TEXT', 'index' => FALSE]
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
                $query .= " metodo_last_update_time = '" . $oldDate->format("c") . "'";
            }
            $query .= ($local && $remote) ? ',' : '';
            if ($remote) {
                $query .= " crm_last_update_time = '" . $oldDate->format("c") . "'";
                $query .= ", crm_id = NULL";
            }
            $query .= ";";
            $statement = $this->db->prepare($query);
            $statement->execute();
        }
    }
}