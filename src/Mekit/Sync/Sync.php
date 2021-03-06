<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 18.12
 */

namespace Mekit\Sync;


use Monolog\Logger;

class Sync {
    /** @var callable */
    protected $logger;

    /**
     * @param callable $logger
     */
    public function __construct($logger) {
        $this->logger = $logger;
    }

    /**
     * @param string $msg
     * @param int    $level
     * @param array  $context
     */
    protected function log($msg, $level = Logger::INFO, $context = []) {
        call_user_func($this->logger, $msg, $level, $context);
    }
}