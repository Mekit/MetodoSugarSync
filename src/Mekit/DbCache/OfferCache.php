<?php
/**
 * Created by Adam Jakab.
 * Date: 17/12/15
 * Time: 12.41
 */

namespace Mekit\DbCache;

class OfferCache extends CacheDb {
    /** @var array */
    protected $columns = [
        'id' => ['type' => 'TEXT', 'index' => TRUE, 'unique' => TRUE],
        'crm_id' => ['type' => 'TEXT', 'index' => FALSE],
        'database_metodo' => ['type' => 'TEXT', 'index' => TRUE],
        'id_head' => ['type' => 'TEXT', 'index' => TRUE],
        'document_number' => ['type' => 'TEXT', 'index' => FALSE],
        'data_doc' => ['type' => 'TEXT', 'index' => FALSE],
        'cod_c_f' => ['type' => 'TEXT', 'index' => FALSE],
        'imp_agent_code' => ['type' => 'TEXT', 'index' => FALSE],
        'mekit_agent_code' => ['type' => 'TEXT', 'index' => FALSE],
        'dsc_payment' => ['type' => 'TEXT', 'index' => FALSE],
        'tot_imponibile_euro' => ['type' => 'TEXT', 'index' => FALSE],
        'tot_imposta_euro' => ['type' => 'TEXT', 'index' => FALSE],
        'tot_documento_euro' => ['type' => 'TEXT', 'index' => FALSE],
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
            }
            $query .= ";";
            $statement = $this->db->prepare($query);
            $statement->execute();
        }
    }
}