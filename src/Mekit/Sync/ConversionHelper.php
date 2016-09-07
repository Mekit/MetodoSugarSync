<?php
/**
 * Created by Adam Jakab.
 * Date: 06/04/16
 * Time: 11.00
 */

namespace Mekit\Sync;

/**
 * Class ConversionHelper
 * @package Mekit\Sync
 */
class ConversionHelper
{
  /**
   * @var string
   */
  protected static $MSSQL_ALLOWED_CHARS_PATTERN = "#[^a-zA-Z0-9 _\\-\\(\\)\\[\\]\\/\\'.:,;*!&@]#";// (-_.,;*!&@)

  /**
   * @var string
   */
  protected static $MSSQL_UNALLOWED_CHAR_REPLACEMENT = '';

  /**
   * @param mixed  $value
   * @param string $pattern
   * @return mixed
   */
  public static function cleanupMSSQLFieldValue($value, $pattern = '')
  {
    if (!empty($value))
    {
      $value = self::codepoint_decode($value);
      $value = htmlspecialchars_decode($value, ENT_QUOTES);//convert "&#039;" back to "'"
      $value = htmlspecialchars_decode($value, ENT_NOQUOTES);//convert "&quot;" back to '"'
      $pattern = ($pattern ? $pattern : self::$MSSQL_ALLOWED_CHARS_PATTERN);
      $value = preg_replace($pattern, self::$MSSQL_UNALLOWED_CHAR_REPLACEMENT, $value);
    }
    return $value;
  }


  /**
   * @param string $value
   * @return string
   */
  public static function cleanupSuiteCRMFieldValue($value)
  {
    $value = preg_replace('/[^A-Za-z0-9. -_]/', '*', $value);
    return $value;
  }

  /**
   * @param mixed $value
   * @return string
   */
  public static function hexEncodeDataForMSSQL($value)
  {
    $pos = strpos($value, '\'');
    if (!is_bool($pos))
    {
      $unpacked = unpack('H*hex', $value);
      $value = "0x" . $unpacked['hex'];
    }
    return $value;
  }

  /**
   * @param string $str
   * @return string
   */
  protected static function codepoint_encode($str)
  {
    return substr(json_encode($str), 1, -1);
  }

  /**
   * @param string $str
   * @return mixed
   */
  protected static function codepoint_decode($str)
  {
    return json_decode(sprintf('"%s"', $str));
  }

}