<?php
/**
 * Created by Adam Jakab.
 * Date: 01/12/15
 * Time: 11.32
 */

namespace Mekit\Sync\Metodo\Down;


use Mekit\Console\Configuration;

class AccountData {
  /** @var callable */
  protected $logger;

  /**
   * @param callable $logger
   */
  public function __construct($logger) {
    $this->logger = $logger;
  }


  public function execute() {
    $this->log("getting account data from Metodo...");

  }

  protected function getData() {
    $db = Configuration::getDatabaseConnection("SERVER2K8");
    $query = "SELECT TOP 10 * FROM [SogCRM_TesteDocumenti] WHERE [TipoDoc] = 'DDT'";


    try {
      $query = "SELECT TOP 1000 [CodiceMetodo] FROM [IMP].[dbo].[SogCRM_AnagraficaCF]";
      $statement = $db->prepare($query);
      $statement->execute();
      $result = $statement->fetchAll(PDO::FETCH_ASSOC);
    } catch(\Exception $exception) {
      die("Unable to execute query!\n" . $exception->getMessage() . "\n");
    }

    //echo "Documents: " . count($result);

    print_r($result);
    echo "\n";

  }

  protected function log($msg) {
    call_user_func($this->logger, $msg);
  }
}