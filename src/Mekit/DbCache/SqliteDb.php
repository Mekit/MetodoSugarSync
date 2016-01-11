<?php
namespace Mekit\DbCache;

use Mekit\Console\Configuration;

class SqliteDb {
    /** @var  string */
    protected $dataIdentifier;

    /** @var  string */
    protected $dbPath;

    /** @var  \PDO */
    protected $db;

    /** @var  callable */
    private $logger;

    /**
     * @param string   $dataIdentifier
     * @param callable $logger
     */
    public function __construct($dataIdentifier, $logger) {
        $this->dataIdentifier = $dataIdentifier;
        $this->logger = $logger;
        $dbName = $dataIdentifier;
        /*
         * If $dataIdentifier is like Contact_codes - then the name of the database will be the first part
         * before the underscore("_")
         * */
        if (strpos($dbName, "_") !== FALSE) {
            $parts = explode("_", $dbName);
            $dbName = $parts[0];
        }
        $cfg = Configuration::getConfiguration();
        $this->dbPath = $cfg["global"]["temporary_path"] . '/' . $dbName . '.sqlite';
        $this->log("Initialized Cache($dataIdentifier): " . $this->dbPath);
        $this->db = new \PDO('sqlite:' . $this->dbPath);
        $this->db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
    }

    /**
     * @param string $msg
     */
    protected function log($msg) {
        call_user_func($this->logger, $msg);
    }
}