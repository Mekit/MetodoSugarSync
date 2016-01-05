<?php
/**
 * Created by PhpStorm.
 * User: jackisback
 * Date: 14/11/15
 * Time: 22.32
 */

namespace Mekit\Console;

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
        $cfg = self::getConfiguration();
        if (!isset($cfg["database"][$databaseName]) || !is_array($cfg["database"][$databaseName])) {
            throw new \LogicException("Missing configuration for $databaseName in 'database' section!");
        }
        $serverType = $cfg["database"][$databaseName]["type"];
        $serverName = $cfg["database"][$databaseName]["servername"];
        $username = $cfg["database"][$databaseName]["username"];
        $password = $cfg["database"][$databaseName]["password"];
        switch ($serverType) {
            case "MSSQL":
                $connection = new \PDO("odbc:$serverName", "$username", "$password");
                break;
            default:
                throw new \LogicException("The server type($serverType) for $databaseName is not recognized!");
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
            throw new \InvalidArgumentException("Malformed configuration file!" . $configPath);
        }

        $imports = [];
        if (isset($config["imports"]) && is_array($config["imports"]) && count($config["imports"])) {
            $imports = $config["imports"];
            unset($config["imports"]);
        }


        foreach ($imports as $import) {
            if (isset($import["resource"])) {
                $resourcePath = realpath(dirname($configPath) . '/' . $import["resource"]);
                if ($resourcePath) {
                    $additionalConfig = $yamlParser->parse(file_get_contents($resourcePath));
                    $config = array_replace_recursive($additionalConfig, $config);
                }
                else {
                    throw new \LogicException(
                        "Import resource is set but cannot be found(" . $import["resource"] . ")!"
                    );
                }
            }
        }

        $config = $config["configuration"];
        //print_r($config);

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