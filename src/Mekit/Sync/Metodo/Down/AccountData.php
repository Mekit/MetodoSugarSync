<?php
/**
 * Created by Adam Jakab.
 * Date: 01/12/15
 * Time: 11.32
 */

namespace Mekit\Sync\Metodo\Down;


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
    $query = "SELECT TOP 10 * FROM [SogCRM_TesteDocumenti] WHERE [TipoDoc] = 'DDT'";
  }

  protected function log($msg) {
    call_user_func($this->logger, $msg);
  }
}