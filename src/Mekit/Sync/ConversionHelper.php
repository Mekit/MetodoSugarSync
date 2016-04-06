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
        $value = htmlspecialchars_decode($value, ENT_QUOTES);//convert "&#039;" back to "'"
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
}