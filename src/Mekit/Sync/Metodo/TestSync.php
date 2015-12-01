<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 17.45
 */

namespace Mekit\Sync\Metodo;

use Mekit\Sync\SyncInterface;

class TestSync extends MetodoSync implements SyncInterface
{
  const SYNC_NAME = 'test-sync';

  /**
   * @param callable $logger
   */
  public function __construct($logger) {
    parent::__construct($logger);
  }

  public function execute() {
    $this->log('Executing: ' . static::SYNC_NAME . "...");
    $this->getData();

    $this->log(static::SYNC_NAME . " done.");
  }

  protected function getData() {
    $query = "SELECT TOP 10 * FROM [SogCRM_TesteDocumenti] WHERE [TipoDoc] = 'DDT'";


  }

}