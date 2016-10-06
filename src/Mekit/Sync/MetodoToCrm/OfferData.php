<?php
/**
 * Created by Adam Jakab.
 * Date: 05/01/16
 * Time: 14.34
 */

namespace Mekit\Sync\MetodoToCrm;

use Mekit\Console\Configuration;
use Mekit\DbCache\OfferCache;
use Mekit\DbCache\OfferLineCache;
use Mekit\DbCache\ProductCache;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRest;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRestException;
use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;
use Monolog\Logger;

class OfferData extends Sync implements SyncInterface {
    /** @var callable */
    protected $logger;

    /** @var SugarCrmRest */
    protected $sugarCrmRest;

    /** @var  OfferCache */
    protected $offerCacheDb;

    /** @var OfferLineCache */
    protected $offerLineCacheDb;

    /** @var ProductCache */
    protected $productCache;

    /** @var  \PDOStatement */
    protected $localItemStatement;

    /** @var  \PDOStatement */
    protected $localItemStatement2;

    /** @var array */
    protected $userIdCache;

    /** @var array */
    protected $counters = [];

    /** @var array */
    protected $product_codes_vat_10 = ['FESEF002', 'PHF046', 'PHAC75', 'PHF004'];

    /** @var string */
    protected $offer_sync_limit_date = '2015-01-01';

    /**
     * @param callable $logger
     */
    public function __construct($logger) {
        parent::__construct($logger);
        $this->offerCacheDb = new OfferCache('Offers', $logger);
        $this->offerLineCacheDb = new OfferLineCache('Offers_Lines', $logger);
        $this->productCache = new ProductCache('Products', $logger);
        $this->sugarCrmRest = new SugarCrmRest();
    }

    /**
     * @param array $options
     */
    public function execute($options) {
        //$this->log("EXECUTING..." . json_encode($options));
        if (isset($options["delete-cache"]) && $options["delete-cache"]) {
            $this->offerCacheDb->removeAll();
            $this->offerLineCacheDb->removeAll();
        }

        if (isset($options["invalidate-cache"]) && $options["invalidate-cache"]) {
            $this->offerCacheDb->invalidateAll(TRUE, TRUE);
            $this->offerLineCacheDb->invalidateAll(TRUE, TRUE);
        }

        if (isset($options["invalidate-local-cache"]) && $options["invalidate-local-cache"]) {
            $this->offerCacheDb->invalidateAll(TRUE, FALSE);
            $this->offerLineCacheDb->invalidateAll(TRUE, FALSE);
        }

        if (isset($options["invalidate-remote-cache"]) && $options["invalidate-remote-cache"]) {
            $this->offerCacheDb->invalidateAll(FALSE, TRUE);
            $this->offerLineCacheDb->invalidateAll(FALSE, TRUE);
        }

        if (isset($options["update-cache"]) && $options["update-cache"]) {
            $this->updateLocalCache();
        }

        if (isset($options["update-remote"]) && $options["update-remote"]) {
            $this->updateRemoteFromCache();
        }
    }

    /**
     * get data from Metodo and store it in local cache
     */
    protected function updateLocalCache() {
        $FORCE_LIMIT = FALSE;
        $this->log("updating local cache(offers)...");
        $this->counters["cache"]["index"] = 0;
        foreach (["MEKIT", "IMP"] as $database) {
            while ($localItem = $this->getNextOffer($database)) {
                if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counters["cache"]["index"] >= $FORCE_LIMIT) {
                    break;
                }
                $this->counters["cache"]["index"]++;
                $this->saveOfferInCache($localItem);
            }
        }

        $this->log("updating local cache(offer lines)...");
        $this->counters["cache"]["index"] = 0;
        foreach (["MEKIT", "IMP"] as $database) {
            while ($localItem = $this->getNextOfferLine($database)) {
                if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counters["cache"]["index"] >= $FORCE_LIMIT) {
                    break;
                }
                $this->counters["cache"]["index"]++;
                $this->saveOfferLineInCache($localItem);
            }
        }
    }


    protected function updateRemoteFromCache() {
        //OFFERS
        $FORCE_LIMIT = FALSE;
        $this->log("updating remote...");
        $this->offerCacheDb->resetItemWalker();
        $this->counters["remote"]["index"] = 0;
        while ($cachedOfferItem = $this->offerCacheDb->getNextItem('metodo_last_update_time', $orderDir = 'DESC')) {
            if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counters["remote"]["index"] >= $FORCE_LIMIT) {
                $this->offerCacheDb->resetItemWalker();
                break;
            }
            $this->counters["remote"]["index"]++;

            $cachedOfferLines = $this->offerLineCacheDb->loadItems(
                ['offer_id' => $cachedOfferItem->id],
                ['line_order' => 'ASC']
            );

            $remoteOfferItem = $this->saveOfferOnRemote($cachedOfferItem, $cachedOfferLines);
            $this->storeCrmIdForCachedOffer($cachedOfferItem, $remoteOfferItem);
            $this->saveOfferLinesOnRemote($cachedOfferItem, $cachedOfferLines);
        }
    }



    //--------------------------------------------------------------------------------------------REMOTE SYNC OFFER LINE
    /**
     * @param \stdClass $cachedOfferItem
     * @param array     $cachedOfferLines
     * @return \stdClass|bool
     */
    protected function saveOfferLinesOnRemote($cachedOfferItem, $cachedOfferLines) {
        if (empty($cachedOfferItem->crm_id)) {
            $this->log("OfferLInes - Cached Offer does not have crmid - SKIPPING!");
          return FALSE;
        }
        if (!count($cachedOfferLines)) {
            $this->log("OfferLInes - No lines for this offer - SKIPPING!");
          return FALSE;
        }
      //we will use date of the offer to decide if to update lines
        $ISO = 'Y-m-d\TH:i:sO';
        $metodoLastUpdate = \DateTime::createFromFormat($ISO, $cachedOfferItem->metodo_last_update_time);
        $crmLastUpdate = \DateTime::createFromFormat($ISO, $cachedOfferItem->crm_last_update_time);
        if ($metodoLastUpdate > $crmLastUpdate) {

            $remoteOfferId = $cachedOfferItem->crm_id;

            //get Line Group Id
            try {
                $remoteLineGroupID = $this->loadRemoteOfferLineGroupId($cachedOfferItem);
            } catch(\Exception $e) {
                $this->log("CANNOT LOAD GROUP ID FROM CRM - UPDATE WILL BE SKIPPED: " . $e->getMessage());
              return FALSE;
            }

            //Save Line Group
            $groupData = $this->createGroupDataFromOfferLines($cachedOfferLines, $remoteOfferId, $remoteLineGroupID);
            try {
                $groupData->discount_percent_c = $groupData->discount_percent;
                $remoteLineGroupID = $this->saveOfferLineGroupOnRemote($groupData);
            } catch(\Exception $e) {
                $this->log("CANNOT LOAD GROUP ID FROM CRM - UPDATE WILL BE SKIPPED: " . $e->getMessage());
              return FALSE;
            }

            if (!$remoteLineGroupID) {
                $this->log("OfferLines - No remote group id - SKIPPING!");
              return FALSE;
            }

            /** @var \stdClass $cachedOfferLine */
            foreach ($cachedOfferLines as $cachedOfferLine) {
                $this->saveOfferLineOnRemote($cachedOfferLine, $remoteOfferId, $remoteLineGroupID);
                //$this->log("OfferLines - LINE ID: " . json_encode($remoteLineItem));
            }
          return TRUE;
        }
      return FALSE;
    }

    /**
     * @param \stdClass $cachedOfferLine
     * @param string    $remoteOfferId
     * @param string    $remoteLineGroupID
     * @return bool|\stdClass
     */
    protected function saveOfferLineOnRemote($cachedOfferLine, $remoteOfferId, $remoteLineGroupID) {
        $remoteOfferLineItem = FALSE;
        $ISO = 'Y-m-d\TH:i:sO';
        $metodoLastUpdate = \DateTime::createFromFormat($ISO, $cachedOfferLine->metodo_last_update_time);
        $crmLastUpdate = \DateTime::createFromFormat($ISO, $cachedOfferLine->crm_last_update_time);
        if ($metodoLastUpdate > $crmLastUpdate) {
            $this->log(
                "------------------------------------------------------------------------------:"
                . $cachedOfferLine->id_line
            );

            try {
                $crm_line_id = $this->loadRemoteOfferLineId($cachedOfferLine);
            } catch(\Exception $e) {
                $this->log("CANNOT LOAD LINE ID FROM CRM - UPDATE WILL BE SKIPPED: " . $e->getMessage());
                return $remoteOfferLineItem;
            }

            $syncItem = new \stdClass();

            $syncItem->metodo_database_c = strtolower($cachedOfferLine->database_metodo);
            $syncItem->metodo_id_line_c = $cachedOfferLine->id_line;
            $syncItem->number = $cachedOfferLine->line_order;
            $syncItem->assigned_user_id = 1;

            $syncItem->parent_type = 'AOS_Quotes';
            $syncItem->parent_id = $remoteOfferId;
            $syncItem->group_id = $remoteLineGroupID;

            $syncItem->name = substr($cachedOfferLine->article_description, 0, 128)
                              . (strlen($cachedOfferLine->article_description) > 128 ? '...' : '');
            $syncItem->part_number = $cachedOfferLine->article_code;
            $syncItem->item_description = $cachedOfferLine->article_description;
            $syncItem->pricelist_number_c = $cachedOfferLine->price_list_number;
            $syncItem->measurement_unit_c = $cachedOfferLine->measure_unit;
            $syncItem->currency_id = '-99';

            $calc = $this->getCalculatedValuesForOfferLine($cachedOfferLine);

            if ($calc['product_crm_id']) {
                $syncItem->product_id = $calc['product_crm_id'];
            }

            $syncItem->product_qty = $this->fixCurrency($calc['product_qty']);

            $syncItem->product_cost_price = $this->fixCurrency($calc['product_cost_price']);
            $syncItem->product_cost_price_usdollar = $this->fixCurrency($calc['product_cost_price']);

            $syncItem->product_list_price = $this->fixCurrency($calc['product_list_price_unit']);
            $syncItem->product_list_price_usdollar = $this->fixCurrency($calc['product_list_price_unit']);

            $syncItem->discount = $calc['discount_type'];
            $syncItem->product_discount = $this->fixCurrency($calc['product_discount']);
            $syncItem->product_discount_usdollar = $this->fixCurrency($calc['product_discount']);
            $syncItem->product_discount_amount = $this->fixCurrency($calc['product_discount_amount_unit']);
            $syncItem->product_discount_amount_usdollar = $this->fixCurrency($calc['product_discount_amount_unit']);

            $syncItem->product_unit_price = $this->fixCurrency($calc['product_unit_price']);
            $syncItem->product_unit_price_usdollar = $this->fixCurrency($calc['product_unit_price']);

            $syncItem->vat = $calc['vat'];
            $syncItem->vat_amt = $this->fixCurrency($calc['vat_amt']);
            $syncItem->vat_amt_usdollar = $this->fixCurrency($calc['vat_amt']);

            $syncItem->product_total_price = $this->fixCurrency($calc['product_total_price']);
            $syncItem->product_total_price_usdollar = $this->fixCurrency($calc['product_total_price']);

            //add id to sync item for update
            $restOperation = "INSERT";
            if ($crm_line_id) {
                $syncItem->id = $crm_line_id;
                $restOperation = "UPDATE";
            }

            //create arguments for rest
            $arguments = [
                'module_name' => 'AOS_Products_Quotes',
                'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($syncItem),
            ];
            $this->log("CRM SYNC ITEM[" . $restOperation . "]: " . json_encode($arguments));

            try {
                $remoteOfferLineItem = $this->sugarCrmRest->comunicate('set_entries', $arguments);
                $this->log("REMOTE OfferLine RESULT: " . json_encode($remoteOfferLineItem));
            } catch(SugarCrmRestException $e) {
                //go ahead with false silently
                $this->log("REMOTE ERROR!!! - " . $e->getMessage());
                //we must remove crm_id from $cacheItem
                //create fake result
                $remoteOfferLineItem = new \stdClass();
                $remoteOfferLineItem->updateFailure = TRUE;
            }
        }
        return $remoteOfferLineItem;
    }

    /**
     * @param \stdClass $groupData
     * @return bool|string
     */
    protected function saveOfferLineGroupOnRemote($groupData) {
        $group_id = FALSE;
        $restOperation = (isset($groupData->id) ? "UPDATE" : "INSERT");

        //create arguments for rest
        $arguments = [
            'module_name' => 'AOS_Line_Item_Groups',
            'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($groupData),
        ];

        $this->log("CRM SYNC LINE ITEM GROUP[" . $restOperation . "]: " . json_encode($arguments));

        try {
            $remoteOfferItem = $this->sugarCrmRest->comunicate('set_entries', $arguments);
            //$this->log("REMOTE RESULT: " . json_encode($remoteOfferItem));
            if (isset($remoteOfferItem->ids[0]) && !empty($remoteOfferItem->ids[0])) {
                $group_id = $remoteOfferItem->ids[0];
            }
        } catch(SugarCrmRestException $e) {
            //go ahead with false silently
            $this->log("REMOTE ERROR!!! - " . $e->getMessage());
        }
        return $group_id;
    }

    /**
     * @param array  $cachedOfferLines
     * @param string $crm_offer_id
     * @param string $crm_line_group_id
     * @return \stdClass
     */
    protected function createGroupDataFromOfferLines($cachedOfferLines, $crm_offer_id, $crm_line_group_id) {
        $answer = new \stdClass();

        if (!empty($crm_line_group_id)) {
            $answer->id = $crm_line_group_id;
        }
        $answer->name = 'RIGHE DA METODO';
        $answer->assigned_user_id = '1';
        $answer->number = 1;
        $answer->currency_id = '-99';
        $answer->parent_type = 'AOS_Quotes';
        $answer->parent_id = $crm_offer_id;

        $netTotal = 0;
        $netTotal42 = 0;
        $discountTotal = 0;
        $taxTotal = 0;

        /** @var \stdClass $cachedOfferLine */
        foreach ($cachedOfferLines as $cachedOfferLine) {
            $calc = $this->getCalculatedValuesForOfferLine($cachedOfferLine);
            $netTotal += $calc['product_total_price'];
            $netTotal42 += $calc['product_list_price'];
            $discountTotal += $calc['product_discount_amount'];
            $taxTotal += $calc['vat_amt'];
        }
        $grossTotal = $netTotal + $taxTotal;

        $discountPercent = 0;
        if ($netTotal42 != 0) {
            $discountPercent = (100 * (1 - ($netTotal / $netTotal42)));
        }


        //Totale - Prezzo Da Listino 42
        $answer->total_amt = $this->fixCurrency($netTotal42);
        $answer->total_amt_usdollar = $this->fixCurrency($netTotal42);

        //Sconto
        $answer->discount_amount = $this->fixCurrency($discountTotal);
        $answer->discount_amount_usdollar = $this->fixCurrency($discountTotal);
        $answer->discount_percent = $this->fixCurrency($discountPercent, 1);

        //Imponibile
        $answer->subtotal_amount = $this->fixCurrency($netTotal);
        $answer->subtotal_amount_usdollar = $this->fixCurrency($netTotal);

        //Tasse
        $answer->tax_amount = $this->fixCurrency($taxTotal);
        $answer->tax_amount_usdollar = $this->fixCurrency($taxTotal);

        //ALWAYS ZERO!
        $answer->subtotal_tax_amount = 0;
        $answer->subtotal_tax_amount_usdollar = 0;

        //Totale IVATO
        $answer->total_amount = $this->fixCurrency($grossTotal);
        $answer->total_amount_usdollar = $this->fixCurrency($grossTotal);

        return $answer;
    }

    /**
     * @param \stdClass $cachedOfferLine
     * @return array
     */
    protected function getCalculatedValuesForOfferLine($cachedOfferLine) {
        $answer = [
            'product_crm_id' => NULL,
            'product_qty' => 0,
            'product_cost_price' => 0,
            'product_list_price' => 0,
            'product_list_price_unit' => 0,
            'discount_type' => 'Percentage',
            'product_discount' => 0,
            'product_discount_amount' => 0,
            'product_discount_amount_unit' => 0,
            'product_unit_price' => 0,
            'vat' => '22.0',
            'vat_amt' => 0,
            'product_total_price' => 0
        ];

        //FIND PRODUCT
        $relatedProduct = FALSE;
        if (!empty($cachedOfferLine->article_code)) {
            $candidates = $this->productCache->loadItems(['id' => $cachedOfferLine->article_code]);
            //there should be ONLY ONE
            if (isset($candidates[0])) {
                $relatedProduct = $candidates[0];
                if (!empty($relatedProduct->crm_id)) {
                    $answer['product_crm_id'] = $relatedProduct->crm_id;
                }
            }
        }

        //GENERIC LOCAL VALUES
        $quantity = (float) $cachedOfferLine->quantity;
        $net_unit = !empty($cachedOfferLine->net_unit) ? (float) $cachedOfferLine->net_unit : 0;
        $net_total = !empty($cachedOfferLine->net_total) ? (float) $cachedOfferLine->net_total : 0;
        $net_total_l42 = !empty($cachedOfferLine->net_total_listino_42) ? (float) $cachedOfferLine->net_total_listino_42 : 0;
        $discount_total = $net_total_l42 - $net_total;
        $discount_percent = 0;
        if ($net_total_l42 != 0) {
            $discount_percent = (100 * (1 - ($net_total / $net_total_l42)));
        }
        $tax_rate = (in_array($cachedOfferLine->article_code, $this->product_codes_vat_10) ? 10 : 22);
        $tax_multiplier = $tax_rate / 100;
        $taxTotal = $net_total * $tax_multiplier;

        //QUANTITY
        $answer['product_qty'] = $quantity;

        //Costo Prodotto
        if ($relatedProduct) {
            $product_cost = !empty($relatedProduct->cost) ? (float) $relatedProduct->cost : 0;
            $answer['product_cost_price'] = $product_cost;
        }

        //Prezzo Prodotto - L42
        $answer['product_list_price'] = $net_total_l42;
        if ($quantity != 0) {
            $answer['product_list_price_unit'] = ($net_total_l42 / $quantity);
        }
        else {
            $answer['product_list_price_unit'] = 0;
        }

        //SCONTO
        $answer['product_discount'] = $discount_percent;
        $answer['product_discount_amount'] = (0 - $discount_total);//this must be negative
        if ($quantity != 0) {
            $answer['product_discount_amount_unit'] = (0 - ($discount_total / $quantity));
        }
        else {
            $answer['product_discount_amount_unit'] = 0;
        }

        //Product Unit price
        $answer['product_unit_price'] = $net_unit;

        //TAX
        $answer['vat'] = $tax_rate . '.0';//this is dropdown and needs '22.0' or '10.0'
        $answer['vat_amt'] = $taxTotal;

        //TOTAL PRICE
        $answer['product_total_price'] = $net_total;

        return $answer;
    }


    //-------------------------------------------------------------------------------------------------REMOTE SYNC OFFER
    /**
     * @param \stdClass $cacheItem
     * @param array     $cachedOfferLines
     * @return \stdClass|bool
     */
    protected function saveOfferOnRemote($cacheItem, $cachedOfferLines) {
        $remoteOfferItem = FALSE;
        $ISO = 'Y-m-d\TH:i:sO';
        $metodoLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->metodo_last_update_time);
        $crmLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->crm_last_update_time);

        if ($metodoLastUpdate > $crmLastUpdate) {
            $this->log(
                "-----------------------------------------------------------------------------------------"
                . $this->counters["remote"]["index"]
            );

            try {
                $crm_id = $this->loadRemoteOfferId($cacheItem);
            } catch(\Exception $e) {
                $this->log("CANNOT LOAD ID FROM CRM - UPDATE WILL BE SKIPPED: " . $e->getMessage());
                return $remoteOfferItem;
            }

            $assignedUserId = $this->loadRemoteUserId(
                ($cacheItem->database_metodo == "IMP" ? "$cacheItem->imp_agent_code" : "mekit_agent_code"),
                $cacheItem->database_metodo
            );
            $assignedUserId = ($assignedUserId ? $assignedUserId : 1);//assign to administrator if no user found

            //$syncItem = clone($cacheItem);
            $syncItem = new \stdClass();

            //add id to sync item for update
            $restOperation = "INSERT";
            if ($crm_id) {
                $syncItem->id = $crm_id;
                $restOperation = "UPDATE";
            }

            //modify sync item here
            $syncItem->metodo_database_c = strtolower($cacheItem->database_metodo);
            $syncItem->metodo_id_head_c = $cacheItem->id_head;
            $syncItem->document_number_c = $cacheItem->document_number;
            $syncItem->name = $cacheItem->database_metodo . ' - ' . $cacheItem->document_number;
            $syncItem->assigned_user_id = $assignedUserId;

            $dataDoc = \DateTime::createFromFormat('Y-m-d H:i:s.u', $cacheItem->data_doc);
            $syncItem->data_doc_c = $dataDoc->format("Y-m-d");
            $syncItem->expiration = $dataDoc->format("Y-m-d");
            $syncItem->document_year_c = $dataDoc->format("Y");

            $syncItem->stage = 'Draft';
            $syncItem->approval_status = 'Approved';
            $syncItem->currency_id = '-99';//EUR

            $syncItem->imp_agent_code_c = $this->fixMetodoCode($cacheItem->imp_agent_code, ['A'], TRUE);
            $syncItem->mekit_agent_code_c = $this->fixMetodoCode($cacheItem->mekit_agent_code, ['A'], TRUE);

            $syncItem->dsc_payment_c = $cacheItem->dsc_payment;


            $groupData = $this->createGroupDataFromOfferLines($cachedOfferLines, NULL, NULL);

            //TOTALE BEFORE DISCOUNT
            $syncItem->total_amt = $groupData->total_amt;
            $syncItem->total_amt_usdollar = $groupData->total_amt_usdollar;

            //TOTALE AFTER DISCOUNT
            $syncItem->subtotal_amount = $groupData->subtotal_amount;
            $syncItem->subtotal_amount_usdollar = $groupData->subtotal_amount_usdollar;

            //TOTALE DISCOUNT
            $syncItem->discount_amount = $groupData->discount_amount;
            $syncItem->discount_amount_usdollar = $groupData->discount_amount_usdollar;

            //TOTALE DISCOUNT(%)
            $syncItem->discount_percent_c = $groupData->discount_percent;

            //TOTALE TAX
            $syncItem->tax_amount = $groupData->tax_amount;
            $syncItem->tax_amount_usdollar = $groupData->tax_amount_usdollar;

            //SHIPPING
            $syncItem->shipping_amount = 0;
            $syncItem->shipping_amount_usdollar = 0;
            $syncItem->shipping_tax = 0;
            $syncItem->shipping_tax_amt = 0;
            $syncItem->shipping_tax_amt_usdollar = 0;

            //TOTALE DOCUMENTO
            $syncItem->total_amount = $groupData->total_amount;
            $syncItem->total_amount_usdollar = $groupData->total_amount_usdollar;


            //create arguments for rest
            $arguments = [
                'module_name' => 'AOS_Quotes',
                'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($syncItem),
            ];

            $this->log("CRM SYNC ITEM[" . $restOperation . "]: " . json_encode($arguments));

            try {
                $remoteOfferItem = $this->sugarCrmRest->comunicate('set_entries', $arguments);
                $this->log("REMOTE RESULT: " . json_encode($remoteOfferItem));
            } catch(SugarCrmRestException $e) {
                //go ahead with false silently
                $this->log("REMOTE ERROR!!! - " . $e->getMessage());
                //we must remove crm_id from $cacheItem
                //create fake result
                $remoteOfferItem = new \stdClass();
                $remoteOfferItem->updateFailure = TRUE;
            }


            if ($remoteOfferItem && isset($remoteOfferItem->ids) && count($remoteOfferItem->ids)) {

                $remoteOfferId = $remoteOfferItem->ids[0];
                $remoteAccountId = FALSE;

                //RELATE TO ACCOUNT
                $res = $this->relateOfferToAccountOnRemote($cacheItem, $remoteOfferId);
                if (!$res) {
                    $this->log("UNABLE TO RELATE OFFER TO ACCOUNT");
                    $remoteOfferItem = new \stdClass();
                    $remoteOfferItem->updateFailure = TRUE;
                }
                else {
                    if (isset($res->ids[0]) && !empty($res->ids[0])) {
                        $remoteAccountId = $res->ids[0];
                    }
                }

                //RELATE TO OPPORTUNITY
                $res = $this->relateOfferToOpportunityOnRemote($cacheItem, $remoteOfferId, $assignedUserId, $remoteAccountId);
                if (!$res) {
                    $this->log("UNABLE TO RELATE OFFER TO OPPORTUNITY");
                    $remoteOfferItem = new \stdClass();
                    $remoteOfferItem->updateFailure = TRUE;
                }
            }
        }
        return $remoteOfferItem;
    }

    /**
     * create/relate to Opportunity
     *
     * @param \stdClass $cacheItem
     * @param string    $remoteOfferId
     * @param string    $assignedUserId
     * @param string    $remoteAccountId
     * @return bool
     * @throws \Exception
     */
    protected function relateOfferToOpportunityOnRemote($cacheItem, $remoteOfferId, $assignedUserId, $remoteAccountId) {
        $remoteOpportunityItem = FALSE;//always return true
        $skipBecauseOfError = FALSE;
        $dataDocumento = \DateTime::createFromFormat('Y-m-d H:i:s.u', $cacheItem->data_doc);
        $fixedDate = \DateTime::createFromFormat("Y-m-d", "2015-09-01");
        if ($dataDocumento->format('U') >= $fixedDate->format('U')) {
            $opportunity_id = FALSE;
            try {
                $opportunity_id = $this->loadRemoteOpportunityId($remoteOfferId, $cacheItem->rif_opportunity_identifier);
            } catch(SugarCrmRestException $e) {
                $skipBecauseOfError = TRUE;
                //keep quiet
            }
            if (!$skipBecauseOfError) {
                if ($opportunity_id === FALSE) {
                    $restOperation = "INSERT";

                    $syncItem = new \stdClass();

                    $dataDoc = \DateTime::createFromFormat('Y-m-d H:i:s.u', $cacheItem->data_doc);

                  //NUMRIFDOC (varchar15)
                  //@todo: usare document_name(NUMRIFDOC) come nome
                    $syncItem->name = 'Opp. su ' . $cacheItem->database_metodo . ' - ' . $dataDoc->format("Y") . ' - '
                                      . $cacheItem->document_number;


                  $syncItem->date_closed = $dataDoc->add(new \DateInterval('P10D'))->format('Y-m-d');
                    $syncItem->statovendita_c = '2';//Offerta
                    $syncItem->amount = $this->fixCurrency($cacheItem->tot_imponibile_euro);
                    $syncItem->amount_usdollar = $this->fixCurrency($cacheItem->tot_imponibile_euro);
                    $syncItem->assigned_user_id = $assignedUserId;
                    $syncItem->account_id = $remoteAccountId;

                    //create arguments for rest
                    $arguments = [
                        'module_name' => 'Opportunities',
                        'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($syncItem),
                    ];

                    $this->log("CRM SYNC OPPORTUNITIES[" . $restOperation . "]: " . json_encode($arguments));

                    try {
                        $remoteOpportunityItem = $this->sugarCrmRest->comunicate('set_entries', $arguments);
                        $this->log("REMOTE RESULT: " . json_encode($remoteOpportunityItem));
                        if (isset($remoteOpportunityItem->ids[0]) && !empty($remoteOpportunityItem->ids[0])) {
                            $opportunity_id = $remoteOpportunityItem->ids[0];
                        }
                    } catch(SugarCrmRestException $e) {
                        //go ahead with false silently
                        $this->log("REMOTE ERROR!!! - " . $e->getMessage());
                    }
                }

                //RELATE OPPORTUNITY TO OFFER
                if ($opportunity_id) {
                    $arguments = [
                        'module_name' => 'AOS_Quotes',
                        'module_id' => $remoteOfferId,
                        'link_field_name' => 'opportunities',
                        'related_ids' => [$opportunity_id]
                    ];
                    try {
                        $result = $this->sugarCrmRest->comunicate('set_relationship', $arguments);
                        //$this->log("RELATIONSHIP RESULT: " . json_encode($result));
                        if (!isset($result->created) || $result->created != 1) {
                            $this->log("OPPORTUNITY RELATIONSHIP ERROR!!! - " . json_encode($arguments));
                            $remoteOpportunityItem = FALSE;
                        }
                        else {
                            $this->log("OPPORTUNITY RELATIONSHIP CREATED!!! - " . json_encode($result));
                            $remoteOpportunityItem = TRUE;//so that cache item is still saved
                        }
                    } catch(SugarCrmRestException $e) {
                        //go ahead with false silently
                        $this->log(
                            "OPPORTUNITY REMOTE ERROR!!! - " . $e->getMessage() . " - " . json_encode($arguments)
                        );
                        $remoteOpportunityItem = FALSE;
                    }
                }
            }
        }
        else {
            $this->log("Skipping Opportunity creation for offer older than: " . $fixedDate->format("Y-m-d"));
            $remoteOpportunityItem = TRUE;//so that cache item is still saved
        }
        return $remoteOpportunityItem;
    }


    /**
     * @param \stdClass $cacheItem
     * @param string    $remoteOfferId
     * @return \stdClass|bool
     * @throws \Exception
     */
    protected function relateOfferToAccountOnRemote($cacheItem, $remoteOfferId) {
        $result = FALSE;

        try {
            $account_id = $this->loadRemoteAccountId($cacheItem);
        } catch(SugarCrmRestException $e) {
            return FALSE;
        }
        if ($account_id) {
            $this->log("Relating Offer(" . $remoteOfferId . ") to Account: " . $account_id);
            $arguments = [
                'module_name' => 'Accounts',
                'module_id' => $account_id,
                'link_field_name' => 'aos_quotes',
                'related_ids' => [$remoteOfferId]
            ];
            try {
                $result = $this->sugarCrmRest->comunicate('set_relationship', $arguments);
                //$this->log("RELATIONSHIP RESULT: " . json_encode($result));
                if (!isset($result->created) || $result->created != 1) {
                    $this->log("RELATIONSHIP ERROR!!! - " . json_encode($arguments));
                }
                else {
                    $result = new \stdClass();
                    $result->ids = [$account_id];
                }
            } catch(SugarCrmRestException $e) {
                //go ahead with false silently
                $this->log("REMOTE ERROR!!! - " . $e->getMessage() . " - " . json_encode($arguments));
            }
        }
        return $result;
    }


    //----------------------------------------------------------------------------------------------------LOAD REMOTE ID
    /**
     * @param \stdClass $cachedOfferItem
     * @return bool
     */
    protected function loadRemoteOfferLineGroupId($cachedOfferItem) {
        $crm_id = FALSE;
        $arguments = [
            'module_name' => 'AOS_Quotes',
            'id' => $cachedOfferItem->crm_id,
            'select_fields' => ['id'],
            'link_name_to_fields_array' => [
                ['name' => 'aos_line_item_groups', 'value' => ['id', 'name']]
            ],
        ];
        $result = $this->sugarCrmRest->comunicate('get_entry', $arguments);

        if (isset($result->relationship_list[0][0]->records[0]->id->value)) {
            $crm_id = $result->relationship_list[0][0]->records[0]->id->value;
            $this->log("FOUND LINE RELATED GROUP ID: " . $crm_id);
        }
        return $crm_id;
    }


    /**
     * They would be recreated all over again
     * @param \stdClass $cacheItem
     * @return string|bool
     * @throws \Exception
     */
    protected function loadRemoteOfferLineId($cacheItem) {
        $arguments = [
            'module_name' => 'AOS_Products_Quotes',
            'query' => "aos_products_quotes_cstm.metodo_id_line_c = '" . $cacheItem->id_line . "'"
                       . " AND aos_products_quotes_cstm.metodo_database_c = '" . strtolower($cacheItem->database_metodo)
                       . "'",
            'order_by' => "",
            'offset' => 0,
            'select_fields' => ['id'],
            'link_name_to_fields_array' => [],
            'max_results' => 2,
            'deleted' => FALSE,
            'Favorites' => FALSE,
        ];

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
            else if (count($result->entry_list) == 1) {
                /** @var \stdClass $remoteItem */
                $remoteItem = $result->entry_list[0];
                $crm_id = $remoteItem->id;
            }
            else {
                $crm_id = FALSE;
            }
        }
        else {
            throw new \Exception("No server response for Crm ID query!" . json_encode($result));
        }
        return ($crm_id);
    }

    /**
     * They would be recreated all over again
     * @param \stdClass $cacheItem
     * @return string|bool
     * @throws \Exception
     */
    protected function loadRemoteOfferId($cacheItem) {
        $arguments = [
            'module_name' => 'AOS_Quotes',
            'query' => "aos_quotes_cstm.metodo_id_head_c = '" . $cacheItem->id_head
                       . "' AND aos_quotes_cstm.metodo_database_c = '"
                       . $cacheItem->database_metodo . "'",
            'order_by' => "",
            'offset' => 0,
            'select_fields' => ['id'],
            'link_name_to_fields_array' => [],
            'max_results' => 2,
            'deleted' => FALSE,
            'Favorites' => FALSE,
        ];


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
            else if (count($result->entry_list) == 1) {
                /** @var \stdClass $remoteItem */
                $remoteItem = $result->entry_list[0];
                $crm_id = $remoteItem->id;
            }
            else {
                $crm_id = FALSE;
            }
        }
        else {
            throw new \Exception("No server response for Crm ID query!" . json_encode($result));
        }
        return ($crm_id);
    }

    /**
     * if 'rif_opportunity_identifier' is set we need to search directly the opportunities filtering by that code
     * otherwise we will check if we already have an opportunity related to this offer
     *
     * @param string $remoteOfferId
     * @param string $rif_opportunity_identifier
     * @return string
     */
    protected function loadRemoteOpportunityId($remoteOfferId, $rif_opportunity_identifier) {
        $crm_id = FALSE;
        if (!empty($rif_opportunity_identifier)) {
            $arguments = [
                'module_name' => 'Opportunities',
                'query' => "opportunities_cstm.identificativo_c" . " = '" . $rif_opportunity_identifier . "'",
                'order_by' => "",
                'offset' => 0,
                'select_fields' => ['id'],
                'link_name_to_fields_array' => [],
                'max_results' => 1,
                'deleted' => FALSE,
                'Favorites' => FALSE,
            ];
            $result = $this->sugarCrmRest->comunicate('get_entry_list', $arguments);
            if (isset($result->entry_list) && count($result->entry_list) == 1) {
                /** @var \stdClass $remoteItem */
                $remoteItem = $result->entry_list[0];
                $this->log(
                    "FOUND REMOTE OPPORTUNITY(BY REFID(" . $rif_opportunity_identifier . ")): "
                    . json_encode($remoteItem)
                );
                $crm_id = $remoteItem->id;
            }
            else {
                $this->log("NO REMOTE OPPORTUNITY: " . json_encode($arguments));
            }
        }
        else {
            $arguments = [
                'module_name' => 'AOS_Quotes',
                'ids' => [$remoteOfferId],
                'select_fields' => ['id', 'name'],
                'link_name_to_fields_array' => [
                    ['name' => 'opportunities', 'value' => ['id', 'name']]
                ],
            ];
            $result = $this->sugarCrmRest->comunicate('get_entries', $arguments);
            if (isset($result->relationship_list[0])) {
                $rel = $this->sugarCrmRest->getNameValueListFromEntyListItem(NULL, $result->relationship_list[0]);
                if (isset($rel->opportunities) && is_array($rel->opportunities) && count($rel->opportunities)) {
                    $relatedOpportunity = $rel->opportunities[0];
                    $this->log("FOUND RELATED OPPORTUNITY: " . json_encode($relatedOpportunity));
                    $crm_id = $relatedOpportunity->id;
                }
            }
        }

        return ($crm_id);
    }

    /**
     * @param string $agentCode
     * @param string $database
     * @return string
     */
    protected function loadRemoteUserId($agentCode, $database) {
        $crm_id = FALSE;
        $fieldName = ($database == 'IMP' ? 'imp_agent_code_c' : 'mekit_agent_code_c');
        $agentCode = $this->fixMetodoCode($agentCode, ["A"], TRUE);
        if (!empty($agentCode)) {
            if (isset($this->userIdCache[$database][$agentCode]) && !empty($this->userIdCache[$database][$agentCode])) {
                $crm_id = $this->userIdCache[$database][$agentCode];
            }
            else {
                $arguments = [
                    'module_name' => 'Users',
                    'query' => "users_cstm." . $fieldName . " = '" . $agentCode . "'",
                    'order_by' => "",
                    'offset' => 0,
                    'select_fields' => ['id'],
                    'link_name_to_fields_array' => [],
                    'max_results' => 1,
                    'deleted' => FALSE,
                    'Favorites' => FALSE,
                ];
                $result = $this->sugarCrmRest->comunicate('get_entry_list', $arguments);
                if (isset($result) && isset($result->entry_list)) {
                    if (count($result->entry_list) == 1) {
                        /** @var \stdClass $remoteItem */
                        $remoteItem = $result->entry_list[0];
                        $this->log("FOUND REMOTE USER: " . json_encode($remoteItem));
                        $crm_id = $remoteItem->id;
                        $this->userIdCache[$database][$agentCode] = $crm_id;
                    }
                    else {
                        $this->log("NO REMOTE USER: " . json_encode($arguments));
                    }
                }
                else {
                    $this->log("NO REMOTE USER: " . json_encode($arguments));
                }
            }
        }
        return ($crm_id);

    }

    /**
     * Remote(CRM) items cannot be identified by ID because if we reset cache table(removing remote crm_id reference)
     * They would be recreated all over again
     * @param \stdClass $cacheItem
     * @return string|bool
     * @throws \Exception
     */
    protected function loadRemoteAccountId($cacheItem) {
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
        $remoteFieldName = $this->getRemoteFieldNameForCodiceMetodo($cacheItem->database_metodo, $cacheItem->cod_c_f);
        $arguments['query'] = "accounts_cstm." . $remoteFieldName . " = '" . $cacheItem->cod_c_f . "'";

        /** @var \stdClass $result */
        $result = $this->sugarCrmRest->comunicate('get_entry_list', $arguments);
        if (isset($result) && isset($result->entry_list)) {
            if (count($result->entry_list) == 1) {
                /** @var \stdClass $remoteItem */
                $remoteItem = $result->entry_list[0];
                $this->log("FOUND REMOTE ACCOUNT: " . json_encode($remoteItem));
                $crm_id = $remoteItem->id;
            }
            else {
                $this->log("NO REMOTE ACCOUNT: " . json_encode($arguments));
            }
        }
        else {
            $this->log("NO REMOTE RESULT: " . json_encode($arguments));
        }
        return ($crm_id);
    }



    //------------------------------------------------------------------------------------------------------CACHE(OFFER)
    /**
     * @param \stdClass $cachedOfferItem
     * @param \stdClass $remoteOfferItem
     */
    protected function storeCrmIdForCachedOffer(&$cachedOfferItem, $remoteOfferItem) {
        if ($remoteOfferItem) {
            $cacheUpdateItem = new \stdClass();
            $cacheUpdateItem->id = $cachedOfferItem->id;
            $remoteItemId = FALSE;
            if (isset($remoteOfferItem->ids[0]) && !empty($remoteOfferItem->ids[0])) {
                $remoteItemId = $remoteOfferItem->ids[0];
            }
            if ((isset($remoteOfferItem->updateFailure) && $remoteOfferItem->updateFailure) || !$remoteItemId) {
                //we must remove crm_id and reset crm_last_update_time on $cacheItem
                $cacheUpdateItem->crm_id = NULL;
                $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
                $cacheUpdateItem->crm_last_update_time = $oldDate->format("c");
            }
            else {
                $cacheUpdateItem->crm_id = $remoteItemId;
                $now = new \DateTime();
                $cacheUpdateItem->crm_last_update_time = $now->format("c");
                //set if also on $cachedOfferItem so that we don't have to reload it
                $cachedOfferItem->crm_id = $remoteItemId;
            }
            $this->offerCacheDb->updateItem($cacheUpdateItem);
        }
    }

    /**
     * @param \stdClass $localItem
     * @throws \Exception
     */
    protected function saveOfferInCache($localItem) {
        /** @var array|bool $operation */
        $operation = FALSE;

        /** @var \stdClass $cachedItem */
        $cachedItem = FALSE;

        /** @var \stdClass $updateItem */
        $updateItem = FALSE;

        /** @var array $warnings */
        $warnings = [];

        /** @var string $itemDb IMP|MEKIT */
        $itemDb = $localItem->database_metodo;

        //get by id_head
        $filter = [
            'database_metodo' => $itemDb,
            'id_head' => $localItem->id_head,
        ];
        $candidates = $this->offerCacheDb->loadItems($filter);
        if ($candidates && count($candidates)) {
            if (count($candidates) > 1) {
                throw new \Exception("Multiple Head Id found for db!");
            }
            $cachedItem = $candidates[0];
            $updateItem = clone($localItem);
            $updateItem->id = $cachedItem->id;
            $updateItem->metodo_last_update_time = $cachedItem->metodo_last_update_time;
            $operation = 'update';
        }

        //not there - create new
        if (!$cachedItem) {
            $updateItem = $this->generateNewOfferObject($localItem);
            $operation = 'insert';
        }

        $metodoLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->metodo_last_update_time);
        $updateItemLastUpdateTime = new \DateTime($updateItem->metodo_last_update_time);
        if ($metodoLastUpdateTime > $updateItemLastUpdateTime) {
            $updateItem->metodo_last_update_time = $metodoLastUpdateTime->format("c");
        }
        else {
            $operation = "skip";
        }

        //DECIDE OPERATION
        //$operation = ($cachedItem == $updateItem) ? "skip" : $operation;


        //LOG
        if ($operation != "skip") {
            $this->log(
                "-----------------------------------------------------------------------------------------"
                . $this->counters["cache"]["index"]
            );
            $this->log("[" . $itemDb . "][" . json_encode($operation) . "]:");
            $this->log("LOCAL: " . json_encode($localItem));
            $this->log("CACHED: " . json_encode($cachedItem));
            $this->log("UPDATE: " . json_encode($updateItem));

        }
        if (!empty($warnings)) {
            foreach ($warnings as $warning) {
                $this->log("WARNING: " . $warning);
            }
        }

        //CHECK
        if (!isset($operation)) {
            throw new \Exception("operation NOT SET!");
        }

        //INSERT / UPDATE OFFER
        switch ($operation) {
            case "insert":
                $this->offerCacheDb->addItem($updateItem);
                break;
            case "update":
                $this->offerCacheDb->updateItem($updateItem);
                break;
            case "skip":
                break;
            default:
                throw new \Exception("Code operation(" . $operation . ") is not implemented!");
        }
    }


    /**
     * @param \stdClass $localItem
     * @return \stdClass
     */
    protected function generateNewOfferObject($localItem) {
        $offer = clone($localItem);
        $offer->id = md5(json_encode($localItem) . microtime(TRUE));
        $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
        $offer->metodo_last_update_time = $oldDate->format("c");
        $offer->crm_last_update_time = $oldDate->format("c");
        return $offer;
    }


    //-------------------------------------------------------------------------------------------------CACHE(OFFER LINE)
    /**
     * @param \stdClass $cacheItem
     * @param \stdClass $remoteItem
     */
    protected function storeCrmIdForCachedOfferLine($cacheItem, $remoteItem) {
        if ($remoteItem) {
            $cacheUpdateItem = new \stdClass();
            $cacheUpdateItem->id = $cacheItem->id;
            $remoteItemId = FALSE;
            if (isset($remoteItem->ids[0]) && !empty($remoteItem->ids[0])) {
                $remoteItemId = $remoteItem->ids[0];
            }
            if (isset($remoteItem->updateFailure) && $remoteItem->updateFailure) {
                //we must remove crm_id and reset crm_last_update_time on $cacheItem
                $cacheUpdateItem->crm_id = NULL;
                $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
                $cacheUpdateItem->crm_last_update_time = $oldDate->format("c");
            }
            else {
                $cacheUpdateItem->crm_id = $remoteItemId;
                $now = new \DateTime();
                $cacheUpdateItem->crm_last_update_time = $now->format("c");
            }
            $this->log("UPDATING CACHED LINE ITEM: " . json_encode($cacheUpdateItem));
            $this->offerLineCacheDb->updateItem($cacheUpdateItem);
        }
    }

    /**
     * @param \stdClass $localItem
     * @throws \Exception
     */
    protected function saveOfferLineInCache($localItem) {
        /** @var array|bool $operation */
        $operation = FALSE;

        /** @var \stdClass $cachedItem */
        $cachedItem = FALSE;

        /** @var \stdClass $updateItem */
        $updateItem = FALSE;

        /** @var array $warnings */
        $warnings = [];

        /** @var string $itemDb IMP|MEKIT */
        $itemDb = $localItem->database_metodo;

        //get by id_head
        $filter = [
            'database_metodo' => $itemDb,
            'id_line' => $localItem->id_line,
        ];
        $candidates = $this->offerLineCacheDb->loadItems($filter);
        if ($candidates && count($candidates)) {
            if (count($candidates) > 1) {
                throw new \Exception("Multiple Line Id found for db!");
            }
            $cachedItem = $candidates[0];
            $updateItem = clone($localItem);
            $updateItem->id = $cachedItem->id;
            $operation = 'update';
        }

        //not there - create new
        if (!$cachedItem) {
            $updateItem = $this->generateNewOfferLineObject($localItem);
            $operation = 'insert';
        }

        $metodoLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->metodo_last_update_time);
        $updateItemLastUpdateTime = new \DateTime($updateItem->metodo_last_update_time);
        if ($metodoLastUpdateTime > $updateItemLastUpdateTime) {
            $updateItem->metodo_last_update_time = $metodoLastUpdateTime->format("c");
        }
        else {
            $operation = "skip";
        }

        //DECIDE OPERATION
        //$operation = ($cachedItem == $updateItem) ? "skip" : $operation;


        //LOG
        if ($operation != "skip") {
            $this->log(
                "-----------------------------------------------------------------------------------------"
                . $this->counters["cache"]["index"]
            );
            $this->log("[" . $itemDb . "][" . json_encode($operation) . "]:");
            $this->log("LOCAL: " . json_encode($localItem));
            $this->log("CACHED: " . json_encode($cachedItem));
            $this->log("UPDATE: " . json_encode($updateItem));

        }
        if (!empty($warnings)) {
            foreach ($warnings as $warning) {
                $this->log("WARNING: " . $warning);
            }
        }

        //CHECK
        if (!isset($operation)) {
            throw new \Exception("operation NOT SET!");
        }

        //INSERT / UPDATE OFFER
        switch ($operation) {
            case "insert":
                $this->offerLineCacheDb->addItem($updateItem);
                break;
            case "update":
                $this->offerLineCacheDb->updateItem($updateItem);
                break;
            case "skip":
                break;
            default:
                throw new \Exception("Code operation(" . $operation . ") is not implemented!");
        }
    }

    /**
     * @param \stdClass $localItem
     * @return \stdClass
     * @throws \Exception
     */
    protected function generateNewOfferLineObject($localItem) {
        $offerLine = clone($localItem);
        $offerLine->id = md5(json_encode($localItem) . microtime(TRUE));
        $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
        $offerLine->metodo_last_update_time = $oldDate->format("c");
        $offerLine->crm_last_update_time = $oldDate->format("c");
        // get offer_id
        $filter = [
            'database_metodo' => $localItem->database_metodo,
            'id_head' => $localItem->id_head,
        ];
        $candidates = $this->offerCacheDb->loadItems($filter);
        if ($candidates && count($candidates)) {
            if (count($candidates) > 1) {
                throw new \Exception("Multiple Offer Head Id found in cache!");
            }
            if (!count($candidates)) {
                throw new \Exception("No Offer found in cache for this line item!");
            }
            $candidate = $candidates[0];
            $offerLine->offer_id = $candidate->id;
        }
        //
        return $offerLine;
    }



    //--------------------------------------------------------------------------------------------------LOAD FROM METODO
    /**
     * @param string $database IMP|MEKIT
     * @return bool|\stdClass
     */
    protected function getNextOffer($database) {
        if (!$this->localItemStatement) {
            $db = Configuration::getDatabaseConnection("SERVER2K8");
            $sql = "SELECT
                TD.PROGRESSIVO AS id_head,
                TD.NUMERODOC AS document_number,
                TD.NUMRIFDOC AS document_name,
                TD.DATADOC AS data_doc,
                TD.CODCLIFOR AS cod_c_f,
                TD.CODAGENTE1 AS " . strtolower($database) . "_agent_code,
                ET.RifOffertaCRM AS rif_opportunity_identifier,
                ISNULL(TP.DESCRIZIONE, '') AS dsc_payment,
                TD.TOTIMPONIBILEEURO * TD.SEGNO AS tot_imponibile_euro,
                TD.TOTIMPOSTAEURO * TD.SEGNO AS tot_imposta_euro,
                TD.TOTDOCUMENTOEURO * TD.SEGNO AS tot_documento_euro,
                TD.DATAMODIFICA AS metodo_last_update_time
                FROM [${database}].[dbo].[TESTEDOCUMENTI] AS TD
                LEFT OUTER JOIN [${database}].[dbo].[TABPAGAMENTI] AS TP ON TD.CODPAGAMENTO = TP.CODICE
                LEFT OUTER JOIN [${database}].[dbo].[EXTRATESTEDOC] AS ET ON ET.IDTESTA = TD.PROGRESSIVO
                WHERE TD.TIPODOC = 'OFC'
                AND TD.DATADOC >= '" . $this->offer_sync_limit_date . "'
            ";
            $this->localItemStatement = $db->prepare($sql);
            $this->localItemStatement->execute();
        }
        $item = $this->localItemStatement->fetch(\PDO::FETCH_OBJ);
        if ($item) {
            $item->database_metodo = $database;
        }
        else {
            $this->localItemStatement = NULL;
        }
        return $item;
    }

    /**
     * @param string $database IMP|MEKIT
     * @return bool|\stdClass
     */
    protected function getNextOfferLine($database) {
        if (!$this->localItemStatement) {
            $db = Configuration::getDatabaseConnection("SERVER2K8");
            $sql = "SELECT
                TD.PROGRESSIVO AS id_head,
                CONCAT(TD.PROGRESSIVO, '-', RD.IDRIGA) AS id_line,
                RD.POSIZIONE AS line_order,
                RD.CODART AS article_code,
                RD.DESCRIZIONEART AS article_description,
                RD.NUMLISTINO AS price_list_number,
                (CASE WHEN RD.TIPORIGA = 'V' THEN 0 ELSE RD.QTAPREZZO * TD.SEGNO END) AS quantity,
                RD.UMPREZZO AS measure_unit,
                RD.TOTNETTORIGAEURO * TD.SEGNO AS net_total,
                RD.PREZZOUNITNETTOEURO * TD.SEGNO AS net_unit,
                CASE WHEN NULLIF(RD.CODART, '') IS NOT NULL THEN
                (CASE WHEN RD.TIPORIGA = 'V' THEN 0 ELSE RD.QTAPREZZO * TD.SEGNO END) * ART.PREZZOEURO
                ELSE 0
                END AS net_total_listino_42,
                RD.DATAMODIFICA AS metodo_last_update_time
                FROM
                [${database}].[dbo].[TESTEDOCUMENTI] AS TD
                INNER JOIN [${database}].[dbo].[RIGHEDOCUMENTI] AS RD ON TD.PROGRESSIVO = RD.IDTESTA
                LEFT OUTER JOIN [${database}].[dbo].[LISTINIARTICOLI] AS ART ON RD.CODART = ART.CODART AND ART.NRLISTINO = 42
                WHERE TD.TIPODOC = 'OFC'
                AND TD.DATADOC >= '" . $this->offer_sync_limit_date . "'
                ORDER BY TD.PROGRESSIVO;
            ";
            $this->localItemStatement = $db->prepare($sql);
            $this->localItemStatement->execute();
        }
        $item = $this->localItemStatement->fetch(\PDO::FETCH_OBJ);
        if ($item) {
            $item->database_metodo = $database;
        }
        else {
            $this->localItemStatement = NULL;
        }
        return $item;
    }

    //-------------------------------------------------------------------------------------------------------------UTILS
    /**
     * @param string $database
     * @param string $codice
     * @return string
     * @throws \Exception
     */
    protected function getRemoteFieldNameForCodiceMetodo($database, $codice) {
        $type = strtoupper(substr($codice, 0, 1));
        switch ($database) {
            case "IMP":
                switch ($type) {
                    case "C":
                        $answer = "imp_metodo_client_code_c";
                        break;
                    case "F":
                        $answer = "imp_metodo_supplier_code_c";
                        break;
                    default:
                        throw new \Exception("Local item needs to have Tipologia C|F!");
                }
                break;
            case "MEKIT":
                switch ($type) {
                    case "C":
                        $answer = "mekit_metodo_client_code_c";
                        break;
                    case "F":
                        $answer = "mekit_metodo_supplier_code_c";
                        break;
                    default:
                        throw new \Exception("Local item needs to have Tipologia C|F!");
                }
                break;
            default:
                throw new \Exception("Local item needs to have database IMP|MEKIT!");
        }
        return $answer;
    }

    /**
     * @param string $originalCode
     * @param array  $prefixes
     * @param bool   $nospace - Do NOT space prefix from number - new crm cannot have spaces in dropdowns
     * @return string
     */
    protected function fixMetodoCode($originalCode, $prefixes, $nospace = FALSE) {
        $normalizedCode = '';
        if (!empty($originalCode)) {
            $codeLength = 7;
            $normalizedCode = '';
            $PREFIX = strtoupper(substr($originalCode, 0, 1));
            $NUMBER = trim(substr($originalCode, 1));
            $SPACES = '';
            if (in_array($PREFIX, $prefixes)) {
                if (0 != (int) $NUMBER) {
                    if (!$nospace) {
                        $SPACES = str_repeat(' ', $codeLength - strlen($PREFIX) - strlen($NUMBER));
                    }
                    $normalizedCode = $PREFIX . $SPACES . $NUMBER;
                }
                else {
                    //$this->log("UNSETTING BAD CODE[not numeric]: '" . $originalCode . "'");
                }
            }
            else {
                //$this->log("UNSETTING BAD CODE[not C|F]: '" . $originalCode . "'");
            }
        }
        return $normalizedCode;
    }

    /**
     * @param string $numberString
     * @param bool|int $decimals
     * @return string
     */
    protected function fixCurrency($numberString, $decimals = FALSE) {
        $numberString = ($numberString ? $numberString : '0');
        if ($decimals) {
            $numberString = number_format((float) $numberString, $decimals);
        }
        $numberString = str_replace('.', ',', $numberString);
        return $numberString;
    }
}