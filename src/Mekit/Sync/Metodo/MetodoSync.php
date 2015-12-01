<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 17.46
 */

namespace Mekit\Sync\Metodo;

use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;

class MetodoSync extends Sync implements SyncInterface
{
  const SYNC_NAME = 'metodo-sync';

  /**
   * @param callable $logger
   */
  public function __construct($logger) {
    parent::__construct($logger);
  }

  public function syncUp() {
    $this->log('Executing: ' . static::SYNC_NAME . '(UP)...');
    $this->log(static::SYNC_NAME . " done.");
  }

  public function syncDown() {
    $this->log('Executing: ' . static::SYNC_NAME . '(DOWN)...');
    $this->log(static::SYNC_NAME . " done.");
  }

  protected function getData() {
    $query = "SELECT TOP 10 * FROM [SogCRM_TesteDocumenti] WHERE [TipoDoc] = 'DDT'";

  }
}