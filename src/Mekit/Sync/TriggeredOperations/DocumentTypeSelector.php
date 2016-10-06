<?php
/**
 * Created by Adam Jakab.
 * Date: 06/10/16
 * Time: 10.33
 */

namespace Mekit\Sync\TriggeredOperations;

use Mekit\Console\Configuration;
use Monolog\Logger;

/**
 * Finds and returns the appropriate name of the class in charge to handle a certain doc type
 *
 * Class DocumentTypeSelector
 * @package Mekit\Sync\TriggeredOperations
 */
class DocumentTypeSelector
{
  /** @var callable */
  protected $logger;

  /** @var  string */
  protected $logPrefix = 'DocumentTypeSelector';

  /** @var array */
  protected $docTypeMap = [];


  public function __construct(callable $logger)
  {
    $this->logger = $logger;
    $this->setupDocTypeMap();
  }

  /**
   * The property 'param1' on $operationElement holds the 3 digit code of the document type
   * which should already be present in $docTypeMap (if not we don't handle it ;))
   *
   * @param \stdClass $operationElement
   * @return string
   * @throws \Exception
   */
  public function getClassNameForDocTypeOperationElement($operationElement)
  {
    if (!isset($operationElement->param1) || empty($operationElement->param1))
    {
      throw new \Exception("Column 'param1' is missing on Operation Element");
    }
    $docType = $operationElement->param1;
    if (!array_key_exists($docType, $this->docTypeMap))
    {
      throw new \Exception("Document type($docType) is not defined in docTypeMap!");
    }

    $docTypeMapItem = $this->docTypeMap[$docType];
    return $docTypeMapItem["doc-type-class-name"];
  }

  /**
   * @throws \Exception
   */
  protected function setupDocTypeMap()
  {
    $cfg = Configuration::getConfiguration();
    if (!isset($cfg["document-type-map"]))
    {
      throw new \Exception("Missing 'document-type-map' key from configuration file!");
    }
    if (!is_array($cfg["document-type-map"]))
    {
      throw new \Exception("The 'document-type-map' key in configuration must be an array!");
    }
    $docTypeMap = [];
    foreach ($cfg["document-type-map"] as $docType => $docTypeClassName)
    {
      $this->checkDocTypeClass($docTypeClassName);
      $docTypeMap[$docType] = [
        'doc-type-class-name' => $docTypeClassName,
      ];
    }
    //$this->log("DOC TYPE MAP: " . json_encode($docTypeMap));

    $this->docTypeMap = $docTypeMap;
  }

  /**
   * @param string $docTypeClassName
   * @throws \Exception
   */
  protected function checkDocTypeClass($docTypeClassName)
  {
    if (!class_exists($docTypeClassName))
    {
      throw new \Exception("Inexistent operation class(" . $docTypeClassName . ")!");
    }
    $reflection = new \ReflectionClass($docTypeClassName);
    if (!$reflection->implementsInterface('Mekit\Sync\TriggeredOperations\TriggeredOperationInterface'))
    {
      throw new \Exception(
        "Operation class(" . $docTypeClassName . ") does not implement TriggeredOperationInterface!"
      );
    }
    if (!$reflection->isSubclassOf('Mekit\Sync\TriggeredOperations\TriggeredOperation'))
    {
      throw new \Exception("Operation class(" . $docTypeClassName . ") does not extend TriggeredOperation!");
    }

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