<?php
/**
 * Created by Adam Jakab.
 * Date: 12/07/16
 * Time: 10.04
 */

namespace Mekit\Sync\TriggeredOperations;


interface TriggeredOperationInterface
{
  /**
   * TriggeredOperationInterface constructor.
   * @param callable  $logger
   * @param \stdClass $operationElement
   */
  public function __construct(callable $logger, \stdClass $operationElement);

  /**
   * @return bool
   */
  public function sync();

  /**
   * @return int
   */
  public function getTaskOnTrigger();
}