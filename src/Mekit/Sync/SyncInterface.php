<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 17.45
 */

namespace Mekit\Sync;


interface SyncInterface {
    /**
     * Main execution method
     * @param array $options
     */
    function execute($options);
}