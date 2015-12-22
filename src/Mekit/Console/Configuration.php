<?php
/**
 * Created by PhpStorm.
 * User: jackisback
 * Date: 14/11/15
 * Time: 22.32
 */

namespace Mekit\Console;

use Doctrine\ORM\EntityManager;
use Doctrine\ORM\Tools\Setup;
use Symfony\Component\Filesystem\Exception\IOException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Yaml\Exception\ParseException;
use Symfony\Component\Yaml\Parser;

class Configuration {
    /** @var  string */
    private static $configurationFilePath;

    /** @var array */
    private static $configuration;

    /**
     * @param string $configurationFilePath
     */
    public static function initializeWithConfigurationFile($configurationFilePath) {
        self::$configurationFilePath = $configurationFilePath;
    }

    /**
     * @return array
     */
    public static function getConfiguration() {
        if (!self::$configuration) {
            self::loadConfiguration();
        }
        return self::$configuration;
    }

    /**
     * @param string $databaseName
     * @return bool|\PDO
     */
    public static function getDatabaseConnection($databaseName) {
        $connection = FALSE;
        $cfg = self::getConfiguration();
        if (!isset($cfg["databases"][$databaseName]) || !is_array($cfg["databases"][$databaseName])) {
            throw new \LogicException("Missing configuration for $databaseName in 'databases' section!");
        }
        $serverType = $cfg["databases"][$databaseName]["type"];
        $serverName = $cfg["databases"][$databaseName]["servername"];
        $username = $cfg["databases"][$databaseName]["username"];
        $password = $cfg["databases"][$databaseName]["password"];
        switch ($serverType) {
            case "MSSQL":
                $connection = new \PDO("odbc:$serverName", "$username", "$password");
                break;
            default:
                throw new \LogicException("The server type($serverType) for $databaseName is not recogniozed!");
        }
        return $connection;
    }

    /**
     * @throws \Exception|ParseException
     */
    protected static function loadConfiguration() {
        $configPath = realpath(self::$configurationFilePath);
        if (!file_exists($configPath)) {
            throw new \InvalidArgumentException("The configuration file does not exist!");
        }
        $fs = new Filesystem();
        $yamlParser = new Parser();
        $config = $yamlParser->parse(file_get_contents($configPath));
        if (!is_array($config) || !isset($config["configuration"])) {
            throw new \InvalidArgumentException("Malformed configuration file!");
        }
        $config = $config["configuration"];

        //Temporary path checks & creation
        if (!isset($config["global"]["temporary_path"])) {
            throw new \LogicException("Missing 'temporary_path' configuration in 'global' section!");
        }
        else {
            if (!$fs->exists($config["global"]["temporary_path"])) {
                try {
                    $fs->mkdir($config["global"]["temporary_path"]);
                } catch(IOException $e) {
                    throw new \LogicException(
                        "Unable to create 'temporary_path'(" . $config["global"]["temporary_path"] . ")!"
                    );
                }
            }
        }

        self::$configuration = $config;
    }


}