<?php
/**
 * Created by Adam Jakab.
 * Date: 17/12/15
 * Time: 12.41
 */

namespace Mekit\DbCache;

class AccountCache extends CacheDb {
    /** @var array */
    protected $columns = [
        'id' => ['type' => 'TEXT', 'index' => TRUE, 'unique' => TRUE],
        'crm_id' => ['type' => 'TEXT', 'index' => FALSE],
        'imp_metodo_client_code_c' => ['type' => 'TEXT', 'index' => TRUE],
        //imp_metodo_client_code_c
        'imp_metodo_supplier_code_c' => ['type' => 'TEXT', 'index' => TRUE],
        //imp_metodo_supplier_code_c
        'mekit_metodo_client_code_c' => ['type' => 'TEXT', 'index' => TRUE],
        //mekit_metodo_client_code_c
        'mekit_metodo_supplier_code_c' => ['type' => 'TEXT', 'index' => TRUE],
        //mekit_metodo_supplier_code_c
        'imp_metodo_invoice_client_c' => ['type' => 'TEXT', 'index' => FALSE],
        //imp_metodo_invoice_client_c
        'imp_metodo_invoice_supplier_c' => ['type' => 'TEXT', 'index' => FALSE],
        //imp_metodo_invoice_supplier_c
        'mekit_metodo_invoice_client_c' => ['type' => 'TEXT', 'index' => FALSE],
        //mekit_metodo_invoice_client_c
        'mekit_metodo_invoice_supplier_c' => ['type' => 'TEXT', 'index' => FALSE],
        //mekit_metodo_invoice_supplier_c
        'partita_iva_c' => ['type' => 'TEXT', 'index' => TRUE],
        'codice_fiscale_c' => ['type' => 'TEXT', 'index' => TRUE],
        'crm_export_flag_c' => ['type' => 'TEXT', 'index' => FALSE],
        'name' => ['type' => 'TEXT', 'index' => FALSE],
        'metodo_last_update_time_c' => ['type' => 'TEXT', 'index' => FALSE],
        'crm_last_update_time_c' => ['type' => 'TEXT', 'index' => FALSE],
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
            $this->log(
                "INVALIDATING CACHE[" . (($local ? "LOCAL" : "") . "|" . ($remote ? "REMOTE" : "")) . "] DATA FOR: "
                . $this->dataIdentifier
            );
            $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
            $query = "UPDATE " . $this->dataIdentifier . " SET";
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