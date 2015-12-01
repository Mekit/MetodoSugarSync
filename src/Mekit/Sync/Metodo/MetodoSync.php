<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 17.46
 */

namespace Mekit\Sync\Metodo;

use Mekit\Sync\Sync;

class MetodoSync extends Sync {
  /**
   * @param callable $logger
   */
  public function __construct($logger) {
    parent::__construct($logger);
  }
}