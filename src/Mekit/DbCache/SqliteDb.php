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
        $cfg = Configuration::getConfiguration();
        $this->dbPath = $cfg["global"]["temporary_path"] . '/' . $this->dataIdentifier . '.sqlite';
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