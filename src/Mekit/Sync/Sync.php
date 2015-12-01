<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 18.12
 */

namespace Mekit\Sync;


class Sync {
  /** @var callable */
  protected $logger;

  /**
   * @param callable $logger
   */
  public function __construct($logger) {
    $this->logger = $logger;
  }

  protected function log($msg) {
    call_user_func($this->logger, $msg);
  }

}