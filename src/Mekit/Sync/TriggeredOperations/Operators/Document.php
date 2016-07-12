<?php
/**
 * Created by Adam Jakab.
 * Date: 12/07/16
 * Time: 10.03
 */

namespace Mekit\Sync\TriggeredOperations\Operators;


use Mekit\Sync\TriggeredOperations\TriggeredOperation;
use Mekit\Sync\TriggeredOperations\TriggeredOperationInterface;

class Document extends TriggeredOperation implements TriggeredOperationInterface
{
  /** @var  string */
  protected $logPrefix = 'Document';

  /**
   *
   */
  public function sync()
  {
    $this->log("SYNCING...");
  }
}