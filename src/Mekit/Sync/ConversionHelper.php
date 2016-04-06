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
class ConversionHelper {

    /**
     * @param mixed $value
     * @return mixed
     */
    public static function cleanupMSSQLFieldValue($value) {
        $value = self::codepoint_decode($value);
        $value = htmlspecialchars_decode($value, ENT_QUOTES);//convert "&#039;" back to "'"
        $value = htmlspecialchars_decode($value, ENT_NOQUOTES);//convert "&quot;" back to '"'
        return $value;
    }

    /**
     * @param mixed $value
     * @return string
     */
    public static function hexEncodeDataForMSSQL($value) {
        $pos = strpos($value, '\'');
        if (!is_bool($pos)) {
            $unpacked = unpack('H*hex', $value);
            $value = "0x" . $unpacked['hex'];
        }
        return $value;
    }

    /**
     * @param string $str
     * @return string
     */
    public static function codepoint_encode($str) {
        return substr(json_encode($str), 1, -1);
    }

    /**
     * @param string $str
     * @return mixed
     */
    public static function codepoint_decode($str) {
        return json_decode(sprintf('"%s"', $str));
    }

}