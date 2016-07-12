<?php
/**
 * Created by Adam Jakab.
 * Date: 12/07/16
 * Time: 10.03
 */

namespace Mekit\Sync\TriggeredOperations\Operators;


use Mekit\Sync\TriggeredOperations\TriggeredOperation;
use Mekit\Sync\TriggeredOperations\TriggeredOperationInterface;

/**
 * A dummy class to be used for unmapped tables with possibility to decide what to do with the
 * operation element
 *
 * Class Dummy
 * @package Mekit\Sync\TriggeredOperations\Operators
 */
class Dummy extends TriggeredOperation implements TriggeredOperationInterface
{
  /** @var  string */
  protected $logPrefix = 'Dummy';

  /**
   * @return bool
   */
  public function sync()
  {
    $this->log("SKIPPING.");
    $this->setTaskOnTrigger(TriggeredOperation::TR_OP_DELETE);
    return FALSE;
  }
}