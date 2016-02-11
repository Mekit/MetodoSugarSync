<?php
/**
 * Created by Adam Jakab.
 * Date: 11/02/16
 * Time: 11.59
 */

namespace Mekit\Sync\CrmToMetodo;

use Mekit\Console\Configuration;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRest;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRestException;
use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;
use Monolog\Logger;


class AccountData extends Sync implements SyncInterface {
    /** @var callable */
    protected $logger;

    /** @var SugarCrmRest */
    protected $sugarCrmRest;

    /** @var array */
    protected $counters = [];

    /** @var array */
    protected $remoteDataOffset = 0;

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
        $this->updateMetodoFromCrm();
    }

    protected function updateMetodoFromCrm() {
        $this->counters["remote"]["index"] = 0;
        while ($remoteItem = $this->getNextRemoteItem()) {
            $this->log("\nREMOTE ITEM: " . json_encode($remoteItem));
        }
    }

    /*

*/
    /**
     * @return \stdClass|bool
     */
    protected function getNextRemoteItem() {
        $answer = FALSE;
        $arguments = [
            'module_name' => 'Accounts',
            'query' => "accounts_cstm.imp_metodo_sync_up_c = 1 OR accounts_cstm.mekit_metodo_sync_up_c = 1",
            'order_by' => '',
            'offset' => $this->remoteDataOffset,
            'select_fields' => [
                'id',
                'name',
                'phone_office',
                'phone_fax',
                'billing_address_street',
                'billing_address_postalcode',
                'billing_address_city',
                'billing_address_state',
                'billing_address_country',
                'website'
            ],
            'link_name_to_fields_array' => [
                ['name' => 'email_addresses', 'value' => ['email_address', 'opt_out', 'primary_address']]
            ],
            'max_results' => 1,
            'deleted' => FALSE,
            'Favorites' => FALSE,
        ];
        /** @var \stdClass $result */
        $result = $this->sugarCrmRest->comunicate('get_entry_list', $arguments);
        print_r($result);
        $entryListItem = (isset($result->entry_list) && isset($result->entry_list[0])) ? $result->entry_list[0] : NULL;
        $relationshipListItem = (isset($result->relationship_list)
                                 && isset($result->relationship_list[0])) ? $result->relationship_list[0] : NULL;
        if ($entryListItem) {
            $answer = $this->sugarCrmRest->getNameValueListFromEntyListItem($entryListItem, $relationshipListItem);
            $this->remoteDataOffset = $result->next_offset;
        }
        else {
            $this->remoteDataOffset = 0;
        }
        return $answer;
    }
}