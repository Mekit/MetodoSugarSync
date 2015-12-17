<?php
/**
 * Created by Adam Jakab.
 * Date: 17/12/15
 * Time: 12.41
 */

namespace Mekit\DbCache;

use Mekit\Console\Configuration;

class AccountCache extends CacheDb {
    /**
     * @param string   $dataIdentifier
     * @param callable $logger
     */
    public function __construct($dataIdentifier, $logger) {
        parent::__construct($dataIdentifier, $logger);
        $this->setupDatabase();
    }


    protected function setupDatabase() {
        $cfg = Configuration::getConfiguration();
        if ($cfg["cache"]["force_recreate_tables"]) {
            $this->db->exec("DROP TABLE IF EXISTS " . $this->dataIdentifier);
            $this->log("Cache table($this->dataIdentifier) dropped.");
        }
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
                   . ", metodo_client_code_imp_c TEXT"
                   . ", metodo_supplier_code_imp_c TEXT"
                   . ", metodo_client_code_mekit_c TEXT"
                   . ", metodo_supplier_code_mekit_c TEXT"
                   . ", metodo_invoice_client_code_imp_c TEXT" /* Cliente di Fatturazione IMP */
                   . ", metodo_invoice_client_code_mekit_c TEXT" /* Cliente di Fatturazione MEKIT */
                   . ", partita_iva_c TEXT"
                   . ", codice_fiscale_c TEXT"
                   . ", metodo_last_update_time_c TEXT NOT NULL"
                   . ")";
            $this->db->exec($sql);
            $this->db->exec("CREATE UNIQUE INDEX ID ON " . $this->dataIdentifier . " (id ASC)");
            $this->db->exec("CREATE INDEX CRMID ON " . $this->dataIdentifier . " (crm_id ASC)");
            $this->db->exec("CREATE INDEX IMP_C ON " . $this->dataIdentifier . " (metodo_client_code_imp_c ASC)");
            $this->db->exec("CREATE INDEX IMP_F ON " . $this->dataIdentifier . " (metodo_supplier_code_imp_c ASC)");
            $this->db->exec("CREATE INDEX MKT_C ON " . $this->dataIdentifier . " (metodo_client_code_mekit_c ASC)");
            $this->db->exec("CREATE INDEX MKT_F ON " . $this->dataIdentifier . " (metodo_supplier_code_mekit_c ASC)");
            $this->db->exec("CREATE INDEX P_IVA ON " . $this->dataIdentifier . " (partita_iva_c ASC)");
            $this->db->exec("CREATE INDEX CFIS ON " . $this->dataIdentifier . " (codice_fiscale_c ASC)");
        }
    }
}