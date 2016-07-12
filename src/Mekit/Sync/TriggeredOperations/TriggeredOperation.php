<?php
/**
 * Created by Adam Jakab.
 * Date: 12/07/16
 * Time: 10.04
 */

namespace Mekit\Sync\TriggeredOperations;


use Monolog\Logger;

class TriggeredOperation
{
  /** @var callable */
  protected $logger;

  /** @var string */
  protected $logPrefix = '';

  /** @var  \stdClass */
  protected $operationElement;

  public function __construct(callable $logger, \stdClass $operationElement)
  {
    $this->logger = $logger;
    $this->operationElement = $operationElement;
  }

  /**
   * @param string $msg
   * @param int    $level
   * @param array  $context
   */
  protected function log($msg, $level = Logger::INFO, $context = [])
  {
    $msg = "[" . $this->logPrefix . "]" . $msg;
    call_user_func($this->logger, $msg, $level, $context);
  }
}