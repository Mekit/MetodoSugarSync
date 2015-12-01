<?php
/**
 * Created by Adam Jakab.
 * Date: 07/10/15
 * Time: 17.45
 */

namespace Mekit\Sync;


interface SyncInterface {
  /** Main sync UP method */
  function syncUp();
  /** Main sync DOWN method */
  function syncDown();
}