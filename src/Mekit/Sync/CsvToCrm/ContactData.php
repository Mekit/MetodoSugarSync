<?php
/**
 * Created by Adam Jakab.
 * Date: 22/03/16
 * Time: 16.08
 */

namespace Mekit\Sync\CsvToCrm;


use Mekit\Console\Configuration;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRest;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRestException;
use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;
use League\Csv\Reader;
use Monolog\Logger;

class ContactData extends Sync implements SyncInterface {
    /** @var callable */
    protected $logger;

    /** @var SugarCrmRest */
    protected $sugarCrmRest;

    /** @var array */
    protected $counters = [];


    /**
     * @param callable $logger
     */
    public function __construct($logger) {
        parent::__construct($logger);
        $this->sugarCrmRest = new SugarCrmRest();
    }

    /**
     * @param array $options
     */
    public function execute($options) {
        //$this->log("EXECUTING..." . json_encode($options));

        if (isset($options["update-remote"]) && $options["update-remote"]) {
            $this->updateRemote();
        }
    }

    protected function updateRemote() {
        $cfg = Configuration::getConfiguration();

        $csv = Reader::createFromPath($cfg['global']['datafile']);

        //get the first row, usually the CSV header
        $headers = $csv->fetchOne();

        $i = 1;
        $maxRun = 99999999999;
        while ($csvItem = $csv->fetchOne($i)) {
            $csvItem = $this->indexifyCsvItem($headers, $csvItem);
            $csvItem["account_id"] = $this->loadRemoteAccountId($csvItem);
            $csvItem["contact_id"] = $this->loadRemoteContactId($csvItem);
            if (!$csvItem["account_id"]) {
                throw new \Exception("Account not found for: " . json_encode($csvItem));
            }


            $this->saveContact($csvItem);
            if (!$csvItem["contact_id"]) {
                throw new \Exception("Contact could not be created/found for: " . json_encode($csvItem));
            }

            $this->relateContactToAccount($csvItem);

            $this->log("CSV ITEM($i): " . json_encode($csvItem));
            $i++;
            if ($i >= $maxRun) {
                break;
            }
        }
    }


    /**
     * @param array $csvItem
     */
    protected function relateContactToAccount(&$csvItem) {
        $this->log("CREATING RELATIONSHIP...");
        $arguments = array(
            'module_name' => 'Accounts',
            'module_id' => $csvItem['account_id'],
            'link_field_name' => 'accounts_contacts_imp_acq',
            'related_ids' => [$csvItem['contact_id']],
            'name_value_list' => []
        );
        $result = $this->sugarCrmRest->comunicate('set_relationship', $arguments);
        if ($result && isset($result->created) && $result->created == 1) {
            $this->log("RELATIONSHIP RESULT: " . json_encode($result));
        }
        else {
            $this->log("FAILED RELATIONSHIP: " . json_encode($result));
        }
    }


    /**
     * @param array $csvItem
     */
    protected function saveContact(&$csvItem) {
        $syncItem = new \stdClass();
        $syncItem->gender_c = 2;
        $syncItem->profiling_c = TRUE;//"Da profilare"
        $syncItem->first_name = addslashes($csvItem['Contact-FirstName']);
        $syncItem->last_name = addslashes($csvItem['Contact-LastName']);
        $syncItem->email1 = strtolower(trim($csvItem['Contact-Email']));
        if (isset($csvItem["contact_id"]) && !empty($csvItem["contact_id"])) {
            $syncItem->id = $csvItem["contact_id"];
        }


        //create arguments for rest
        $arguments = [
            'module_name' => 'Contacts',
            'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($syncItem),
        ];

        $this->log("CRM SYNC ITEM: " . json_encode($arguments));

        try {
            $result = $this->sugarCrmRest->comunicate('set_entries', $arguments);
            if (isset($result->ids[0]) && !empty($result->ids[0])) {
                $csvItem["contact_id"] = $result->ids[0];
            }
            $this->log("REMOTE RESULT: " . json_encode($result));
        } catch(SugarCrmRestException $e) {
            //go ahead with false silently
            $this->log("REMOTE ERROR!!! - " . $e->getMessage());
        }

    }

    /**
     * @param array $csvItem
     * @return string|bool
     * @throws \Exception
     */
    protected function loadRemoteAccountId($csvItem) {
        $crm_id = FALSE;
        $arguments = [
            'module_name' => 'Accounts',
            'query' => "",
            'order_by' => "",
            'offset' => 0,
            'select_fields' => ['id'],
            'link_name_to_fields_array' => [],
            'max_results' => 2,
            'deleted' => FALSE,
            'Favorites' => FALSE,
        ];

        //identify by codice metodo - the first one found
        if (isset($csvItem["Account-Name"]) && !empty($csvItem["Account-Name"])) {
            $arguments['query'] = "accounts.name = '" . addslashes($csvItem["Account-Name"]) . "'";
        }

        if (isset($csvItem["Account-ID"]) && !empty($csvItem["Account-ID"])) {
            $arguments['query'] = "accounts.id = '" . addslashes($csvItem["Account-ID"]) . "'";
        }

        //$this->log("IDENTIFYING CRMID BY: " . json_encode($arguments));

        /** @var \stdClass $result */
        $result = $this->sugarCrmRest->comunicate('get_entry_list', $arguments);
        if (isset($result) && isset($result->entry_list)) {
            if (count($result->entry_list) > 1) {
                //This should never happen!!!
                $this->log(str_repeat("-", 120));
                $this->log(str_repeat("-", 120));
                $this->log(str_repeat("-", 120));
                $this->log(
                    "There is a multiple correspondence for requested codes!"
                    . json_encode($arguments), Logger::ERROR, $result->entry_list
                );
                $this->log("RESULTS: " . json_encode($result->entry_list));
                $this->log(str_repeat("-", 120));
                $this->log(str_repeat("-", 120));
                $this->log(str_repeat("-", 120));
                throw new \Exception(
                    "There is a multiple correspondence for requested codes!" . json_encode($arguments)
                );
            }
            if (count($result->entry_list) == 1) {
                /** @var \stdClass $remoteItem */
                $remoteItem = $result->entry_list[0];
                //$this->log("FOUND REMOTE ITEM: " . json_encode($remoteItem));
                $crm_id = $remoteItem->id;
            }
        }
        else {
            throw new \Exception("No server response for Crm ID query!");
        }
        return ($crm_id);
    }


    /**
     * They would be recreated all over again
     * @param array $csvItem
     * @return string|bool
     * @throws \Exception
     */
    protected function loadRemoteContactId($csvItem) {
        $crm_id = FALSE;
        $arguments = [
            'module_name' => 'Contacts',
            'query' => "contacts.first_name = '" . addslashes($csvItem['Contact-FirstName'])
                       . "' AND contacts.last_name = '" . addslashes($csvItem['Contact-LastName']) . "'",
            'order_by' => "",
            'offset' => 0,
            'select_fields' => ['id'],
            'link_name_to_fields_array' => [],
            'max_results' => 2,
            'deleted' => FALSE,
            'Favorites' => FALSE,
        ];

        //$this->log("IDENTIFYING CRMID BY: " . json_encode($arguments));

        /** @var \stdClass $result */
        $result = $this->sugarCrmRest->comunicate('get_entry_list', $arguments);
        if (isset($result) && isset($result->entry_list)) {
            if (count($result->entry_list) > 1) {
                //This should never happen!!!
                $this->log(str_repeat("-", 120));
                $this->log(str_repeat("-", 120));
                $this->log(str_repeat("-", 120));
                $this->log(
                    "There is a multiple correspondence for requested codes!"
                    . json_encode($arguments), Logger::ERROR, $result->entry_list
                );
                $this->log("RESULTS: " . json_encode($result->entry_list));
                $this->log(str_repeat("-", 120));
                $this->log(str_repeat("-", 120));
                $this->log(str_repeat("-", 120));
                throw new \Exception(
                    "There is a multiple correspondence for requested codes!" . json_encode($arguments)
                );
            }
            if (count($result->entry_list) == 1) {
                /** @var \stdClass $remoteItem */
                $remoteItem = $result->entry_list[0];
                //$this->log("FOUND REMOTE ITEM: " . json_encode($remoteItem));
                $crm_id = $remoteItem->id;
            }
        }
        else {
            throw new \Exception("No server response for Crm ID query!");
        }
        return ($crm_id);
    }

    /**
     * @param array $headers
     * @param array $csvItem
     * @return array
     */
    protected function indexifyCsvItem($headers, $csvItem) {
        $answer = [];
        while (count($headers)) {
            $h = array_pop($headers);
            $v = array_pop($csvItem);
            $answer[$h] = $v;
        }
        return $answer;
    }

}