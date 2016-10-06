<?php
/**
 * Created by Adam Jakab.
 * Date: 05/10/16
 * Time: 15.16
 */

namespace Mekit\Sync\TriggeredOperations\Operators\DocTypes;

use Mekit\Console\Configuration;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRestException;
use Mekit\Sync\ConversionHelper;
use Mekit\Sync\TriggeredOperations\TriggeredOperation;
use Mekit\Sync\TriggeredOperations\TriggeredOperationInterface;

class OFC extends TriggeredOperation implements TriggeredOperationInterface
{

  /** @var  string */
  protected $logPrefix = 'Document[OFC]';

  /**
   * @return bool
   */
  public function sync()
  {
    $result = FALSE;

    $this->setTaskOnTrigger(TriggeredOperation::TR_OP_NOTHING);
    $this->log("sync in progress...");

    return $result;
  }

}