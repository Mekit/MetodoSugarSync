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
use Mekit\Sync\TriggeredOperations\Operators\Document;
use Mekit\Sync\TriggeredOperations\TriggeredOperation;
use Mekit\Sync\TriggeredOperations\TriggeredOperationInterface;

class OFC extends Document
{
  /** @var  string */
  protected $logPrefix = 'Document[OFC]';

  /**
   * @param \stdClass $dataElement
   * @throws \Exception
   * @return bool
   */
  protected function crmUpdateItem($dataElement)
  {
    $answer = FALSE;

    if (!$dataElement)
    {
      throw new \Exception("No datElement! Maybe record has been deleted.");
    }

    return $answer;
  }

}