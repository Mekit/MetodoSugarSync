<?php
/**
 * Created by Adam Jakab.
 * Date: 12/07/16
 * Time: 10.03
 */

namespace Mekit\Sync\TriggeredOperations\Operators;

use Mekit\Console\Configuration;
use Mekit\Sync\TriggeredOperations\TriggeredOperation;
use Mekit\Sync\TriggeredOperations\TriggeredOperationInterface;

/**
 * This class for now is specific to RAS type documents - if you need other types
 * you will need to find a way to have type based classes
 *
 * Class Document
 * @package Mekit\Sync\TriggeredOperations\Operators
 */
class Document extends TriggeredOperation implements TriggeredOperationInterface
{
  /** @var  string */
  protected $logPrefix = 'Document';

  /** @var array */
  protected $docTypeMap = [];


  public function __construct(callable $logger, \stdClass $operationElement)
  {
    parent::__construct($logger, $operationElement);
    $this->setupDocTypeMap();
  }

  /**
   * @return bool
   */
  public function sync()
  {
    $result = FALSE;
    $op = TriggeredOperation::TR_OP_DELETE;

    try
    {

      $docTypeOperator = $this->getInstanceForDocTypeOperationElement();
      $docTypeOperator->sync();
      $op = $docTypeOperator->getTaskOnTrigger();
    } catch(\Exception $e)
    {
      $this->log("ERROR: " . $e->getMessage());
    }

    $this->setTaskOnTrigger($op);
    return $result;
  }

  /**
   * The property 'param1' on $operationElement holds the 3 digit code of the document type
   * which should already be present in $docTypeMap (if not we don't handle it ;))
   *
   * @return TriggeredOperationInterface
   * @throws \Exception
   */
  protected function getInstanceForDocTypeOperationElement()
  {
    if (!isset($this->operationElement->param1) || empty($this->operationElement->param1))
    {
      throw new \Exception("Column 'param1' is missing on Operation Element");
    }
    $docType = $this->operationElement->param1;
    if (!array_key_exists($docType, $this->docTypeMap))
    {
      throw new \Exception("Document type($docType) is not defined in docTypeMap!");
    }

    $docTypeMapItem = $this->docTypeMap[$docType];
    $reflection = new \ReflectionClass($docTypeMapItem["doc-type-class-name"]);

    /** @var TriggeredOperationInterface $operatorInstance */
    $operatorInstance = $reflection->newInstanceArgs([$this->logger, $this->operationElement]);

    return $operatorInstance;
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
    if ($reflection->getParentClass()->getName() != 'Mekit\Sync\TriggeredOperations\TriggeredOperation')
    {
      throw new \Exception("Operation class(" . $docTypeClassName . ") does not extend TriggeredOperation!");
    }
  }

}