<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 17.45
 */

namespace Mekit\Sync\Test;

use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;

class TestSync extends Sync implements SyncInterface
{
  const SYNC_NAME = 'test-sync';

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
}