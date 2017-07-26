<?php
/**
 * Created by Adam Jakab.
 * Date: 05/01/16
 * Time: 14.34
 */

namespace Mekit\Sync\MetodoToCrm;

use Mekit\Console\Configuration;
use Mekit\DbCache\ProductCache;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRest;
use Mekit\SugarCrm\Rest\v4_1\SugarCrmRestException;
use Mekit\Sync\Sync;
use Mekit\Sync\SyncInterface;
use Monolog\Logger;

class ProductData extends Sync implements SyncInterface {
    /** @var callable */
    protected $logger;

    /** @var SugarCrmRest */
    protected $sugarCrmRest;

    /** @var  ProductCache */
    protected $productCache;

    /** @var  \PDOStatement */
    protected $localItemStatement;

    /** @var array */
    protected $categoryTree = [];

    /** @var array */
    protected $counters = [];

    /**
     * @param callable $logger
     */
    public function __construct($logger) {
        parent::__construct($logger);
        $this->productCache = new ProductCache('Products', $logger);
        $this->sugarCrmRest = new SugarCrmRest();
    }

    /**
     * @param array $options
     * @param array $arguments
     */
  public function execute($options, $arguments)
  {
        //$this->log("EXECUTING..." . json_encode($options));
        if (isset($options["delete-cache"]) && $options["delete-cache"]) {
            $this->productCache->removeAll();
        }

        if (isset($options["invalidate-cache"]) && $options["invalidate-cache"]) {
            $this->productCache->invalidateAll(TRUE, TRUE);
        }

        if (isset($options["invalidate-local-cache"]) && $options["invalidate-local-cache"]) {
            $this->productCache->invalidateAll(TRUE, FALSE);
        }

        if (isset($options["invalidate-remote-cache"]) && $options["invalidate-remote-cache"]) {
            $this->productCache->invalidateAll(FALSE, TRUE);
        }

        if (isset($options["update-cache"]) && $options["update-cache"]) {
            $this->updateLocalCache();
        }

        if (isset($options["update-remote-categories"]) && $options["update-remote-categories"]) {
            $this->updateRemoteCategoriesFromCache();
        }

        if (isset($options["update-remote-products"]) && $options["update-remote-products"]) {
            $this->updateRemoteProductsFromCache();
        }
    }

    /**
     * get data from Metodo and store it in local cache
     */
    protected function updateLocalCache() {
        $FORCE_LIMIT = FALSE;
        $this->log("updating local cache(offers)...");
        $this->counters["cache"]["index"] = 0;
        foreach (["IMP"] as $database) {
            while ($localItem = $this->getNextProduct($database)) {
                if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counters["cache"]["index"] >= $FORCE_LIMIT) {
                    break;
                }
                $this->counters["cache"]["index"]++;
                $this->saveProductInCache($localItem);
            }
        }
    }


    protected function updateRemoteCategoriesFromCache() {
        $this->log("updating remote(product categories)...");
        $this->productCache->resetItemWalker();
        while ($cacheItem = $this->productCache->getNextItem()) {
            $this->buildCategoryTree($cacheItem);
        }
        $this->registerRemoteProductCategories();
    }

    protected function updateRemoteProductsFromCache() {
        $this->log("updating remote(products)...");
        $this->productCache->resetItemWalker();
        while ($cacheItem = $this->productCache->getNextItem()) {
            $this->buildCategoryTree($cacheItem);
        }

        $FORCE_LIMIT = FALSE;
        $this->log("updating remote(products)...");
        $this->productCache->resetItemWalker();
        $this->counters["remote"]["index"] = 0;
        while ($cacheItem = $this->productCache->getNextItem()) {
            if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counters["remote"]["index"] >= $FORCE_LIMIT) {
                $this->productCache->resetItemWalker();
                break;
            }
            $this->counters["remote"]["index"]++;
            $remoteProductItem = $this->saveProductOnRemote($cacheItem);
            $remoteRelationItem = $this->saveRelationshipProductToCategoryOnRemote($remoteProductItem, $cacheItem);
            if ($remoteProductItem && $remoteRelationItem) {
                $this->storeCrmIdForCachedProduct($cacheItem, $remoteProductItem);
            }
        }
    }

    /**
     * @param \stdClass $remoteProductItem
     * @param \stdClass $cacheItem
     * @return bool|\stdClass
     */
    protected function saveRelationshipProductToCategoryOnRemote($remoteProductItem, $cacheItem) {
        $result = FALSE;
        if (!$remoteProductItem || !isset($remoteProductItem->ids) || !count($remoteProductItem->ids)) {
            return FALSE;
        }

        $remoteProductId = $remoteProductItem->ids[0];
        $remoteProductCategoryId = $this->getCrmIdForCategory($cacheItem->cat_sub_id);
        if (!$remoteProductId || !$remoteProductCategoryId) {
            return FALSE;
        }
        //$this->log("Remote product(".$cacheItem->id.") id: " . $remoteProductId);
        //$this->log("Remote category(".$cacheItem->cat_sub_name.") id: " . $remoteProductCategoryId);


        //product_categories
        $arguments = [
            'module_name' => 'AOS_Product_Categories',
            'module_id' => $remoteProductCategoryId,
            'link_field_name' => 'aos_products',
            'related_ids' => [$remoteProductId]
        ];
        try {
            $result = $this->sugarCrmRest->comunicate('set_relationship', $arguments);
            $this->log("RELATIONSHIP RESULT: " . json_encode($result));
            if (!isset($result->created) || $result->created != 1) {
                $this->log("RELATIONSHIP ERROR!!! - " . json_encode($arguments));
            }
        } catch(SugarCrmRestException $e) {
            //go ahead with false silently
            $this->log("REMOTE ERROR!!! - " . $e->getMessage() . " - " . json_encode($arguments));
        }
        return $result;
    }

    /**
     * @param \stdClass $cacheItem
     * @return bool|\stdClass
     */
    protected function saveProductOnRemote($cacheItem) {
        $result = FALSE;
        $ISO = 'Y-m-d\TH:i:sO';
        $metodoLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->metodo_last_update_time_c);
        $crmLastUpdate = \DateTime::createFromFormat($ISO, $cacheItem->crm_last_update_time_c);

        if ($metodoLastUpdate > $crmLastUpdate) {
            $this->log(
                "-----------------------------------------------------------------------------------------"
                . $this->counters["remote"]["index"]
            );

            try {
                $crm_id = $this->loadRemoteProductId($cacheItem->id);
            } catch(\Exception $e) {
                $this->log("CANNOT LOAD ID FROM CRM - UPDATE WILL BE SKIPPED");
                return $result;
            }

            $syncItem = clone($cacheItem);
            unset($syncItem->crm_id);
            unset($syncItem->id);

            //add id to sync item for update
            $restOperation = "INSERT";
            if ($crm_id) {
                $syncItem->id = $crm_id;
                $restOperation = "UPDATE";
            }

            $syncItem->part_number = $cacheItem->id;
            $syncItem->crm_last_update_time_c = $metodoLastUpdate->format("Y-m-d H:i:s");
            $syncItem->name = $syncItem->description;
            unset($syncItem->description);

            $syncItem->cost = $this->fixCurrency($syncItem->cost);
            $syncItem->cost_usdollar = $this->fixCurrency($syncItem->cost);
            $syncItem->price = $this->fixCurrency($syncItem->price);
            $syncItem->price_usdollar = $this->fixCurrency($syncItem->price);

            $syncItem->price_lst_9997_c = $this->fixCurrency($syncItem->price_lst_9997_c);
            $syncItem->price_lst_10000_c = $this->fixCurrency($syncItem->price_lst_10000_c);
            $syncItem->sold_last_120_days_c = $this->fixCurrency($syncItem->sold_last_120_days_c);

            //create arguments for rest
            $arguments = [
                'module_name' => 'AOS_Products',
                'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($syncItem),
            ];

            $this->log("CRM SYNC ITEM[" . $restOperation . "]: " . json_encode($arguments));

            try {
                $result = $this->sugarCrmRest->comunicate('set_entries', $arguments);
                $this->log("PRODUCT UPDATE RESULT: " . json_encode($result));
            } catch(SugarCrmRestException $e) {
                //go ahead with false silently
                $this->log("REMOTE ERROR!!! - " . $e->getMessage());
                //we must remove crm_id from $cacheItem
                //create fake result
                $result = new \stdClass();
                $result->updateFailure = TRUE;
            }
        }
        return $result;
    }




    /**
     *
     */
    protected function registerRemoteProductCategories() {
        //PARENT CATEGORIES
        foreach ($this->categoryTree as $mainCatId => &$mainCat) {
            try {
                $crm_id = $this->loadRemoteProductCategoryId($mainCatId);
                if ($crm_id === FALSE) {
                    $data = new \stdClass();
                    $data->name = $mainCat->name;
                    $data->metodo_category_code_c = $mainCatId;
                    $data->is_parent = 1;
                    $arguments = [
                        'module_name' => 'AOS_Product_Categories',
                        'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($data),
                    ];
                    try {
                        $result = $this->sugarCrmRest->comunicate('set_entries', $arguments);
                        if (isset($result->ids) && is_array($result->ids) && count($result->ids)
                            && !empty($result->ids[0])
                        ) {
                            $mainCat->crm_id = $result->ids[0];
                        }
                    } catch(SugarCrmRestException $e) {
                        //go ahead with false silently
                        $this->log("REMOTE ERROR!!! - " . $e->getMessage());
                    }
                }
                else {
                    //already registered
                    $mainCat->crm_id = $crm_id;
                }
                if (isset($mainCat->name) && isset($mainCat->crm_id)) {
                    $this->log("PRODUCT MAIN CATEGORY('" . $mainCat->name . "') ID: " . $mainCat->crm_id);
                }
            } catch(\Exception $e) {
                $this->log("CANNOT LOAD ID FROM CRM - UPDATE WILL BE SKIPPED FOR: " . $mainCat->name);
            }
        }

        //CHILD CATEGORIES
        foreach ($this->categoryTree as $mainCatId => &$mainCat) {
            if (isset($mainCat->crm_id)) {
                $mainCatCrmId = $mainCat->crm_id;
                foreach ($mainCat->children as $childCatId => &$childCat) {
                    try {
                        $childCategoryCreated = FALSE;
                        $crm_id = $this->loadRemoteProductCategoryId($childCatId);
                        if ($crm_id === FALSE) {
                            $data = new \stdClass();
                            $data->name = $childCat->name;
                            $data->metodo_category_code_c = $childCatId;
                            $data->is_parent = 0;
                            $arguments = [
                                'module_name' => 'AOS_Product_Categories',
                                'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($data),
                            ];
                            try {
                                $result = $this->sugarCrmRest->comunicate('set_entries', $arguments);
                                if (isset($result->ids) && is_array($result->ids) && count($result->ids)
                                    && !empty($result->ids[0])
                                ) {
                                    $childCat->crm_id = $result->ids[0];
                                    $childCategoryCreated = TRUE;
                                }
                            } catch(SugarCrmRestException $e) {
                                //go ahead with false silently
                                $this->log("REMOTE ERROR!!! - " . $e->getMessage());
                            }
                        }
                        else {
                            //already registered
                            $childCat->crm_id = $crm_id;
                        }
                        if (isset($childCat->name) && isset($childCat->crm_id)) {
                            $this->log(
                                "PRODUCT CHILD CATEGORY('" . $childCat->name . "') ID: " . $childCat->crm_id
                                . " MAIN CATEGORY ID: " . $mainCatCrmId
                            );
                        }


                        //NOW CREATE RELATIONSHIPS ONLY FOR NEWLY CREATED CHILDREN
                        if ($childCategoryCreated) {
                            $arguments = [
                                'module_name' => 'AOS_Product_Categories',
                                'module_id' => $childCat->crm_id,
                                'link_field_name' => 'parent_category', //''sub_categories',//parent_category_id
                                'related_id' => $mainCatCrmId
                            ];
                            try {
                                $result = $this->sugarCrmRest->comunicate('set_relationship', $arguments);
                                $this->log("RELATIONSHIP RESULT: " . json_encode($result));
                                if (!isset($result->created) || $result->created != 1) {
                                    $this->log("RELATIONSHIP ERROR!!! - " . json_encode($arguments));
                                }
                            } catch(SugarCrmRestException $e) {
                                //go ahead with false silently
                                $this->log("REMOTE ERROR!!! - " . $e->getMessage() . " - " . json_encode($arguments));
                            }
                        }

                    } catch(\Exception $e) {
                        $this->log("CANNOT LOAD CATEGORY ID FROM CRM - UPDATE WILL BE SKIPPED FOR: " . $childCat->name);
                    }
                }
            }
        }
    }

    /**
     * AOS_Products
     * @param string $productId
     * @return string
     * @throws \Exception
     */
    protected function loadRemoteProductId($productId) {
        $arguments = [
            'module_name' => 'AOS_Products',
            'query' => "part_number = '" . $productId . "'",
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
     * AOS_Product_Categories
     * @param string $categoryId
     * @return string
     * @throws \Exception
     */
    protected function loadRemoteProductCategoryId($categoryId) {
        $arguments = [
            'module_name' => 'AOS_Product_Categories',
            'query' => "aos_product_categories_cstm.metodo_category_code_c = '" . $categoryId . "'",
            'order_by' => "",
            'offset' => 0,
            'select_fields' => ['id'],
            'link_name_to_fields_array' => [],
            'max_results' => 1,
            'deleted' => FALSE,
            'Favorites' => FALSE,
        ];


        /** @var \stdClass $result */
        $result = $this->sugarCrmRest->comunicate('get_entry_list', $arguments);
        if (isset($result) && isset($result->entry_list)) {
            if (count($result->entry_list)) {
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
     * @param string $categoryId
     * @return bool|string
     */
    protected function getCrmIdForCategory($categoryId) {
        $answer = FALSE;
        $breakout = FALSE;
        foreach ($this->categoryTree as $mainCatId => &$mainCat) {
            if ($mainCatId == $categoryId) {
                if (isset($mainCat->crm_id) && !empty($mainCat->crm_id)) {
                    $answer = $mainCat->crm_id;
                    $breakout = TRUE;
                }
                else {
                    try {
                        $crm_id = $this->loadRemoteProductCategoryId($mainCatId);
                        if ($crm_id != FALSE) {
                            $answer = $crm_id;
                            $mainCat->crm_id = $crm_id;
                        }
                        $breakout = TRUE;
                    } catch(SugarCrmRestException $e) {
                        $breakout = TRUE;
                    }
                }
            }
            else {
                foreach ($mainCat->children as $childCatId => &$childCat) {
                    if ($childCatId == $categoryId) {
                        if (isset($childCat->crm_id) && !empty($childCat->crm_id)) {
                            $answer = $childCat->crm_id;
                            $breakout = TRUE;
                        }
                        else {
                            try {
                                $crm_id = $this->loadRemoteProductCategoryId($childCatId);
                                if ($crm_id != FALSE) {
                                    $answer = $crm_id;
                                    $childCat->crm_id = $crm_id;
                                }
                                $breakout = TRUE;
                            } catch(SugarCrmRestException $e) {
                                $breakout = TRUE;
                            }
                        }
                    }
                    if ($breakout) {
                        break;
                    }
                }
            }
            if ($breakout) {
                break;
            }
        }
        return $answer;
    }


    /**
     * @param \stdClass $cacheItem
     */
    protected function buildCategoryTree($cacheItem) {
        if ($cacheItem->cat_main_id) {
            if (!array_key_exists($cacheItem->cat_main_id, $this->categoryTree)) {
                $this->categoryTree[$cacheItem->cat_main_id] = new \stdClass();
                $this->categoryTree[$cacheItem->cat_main_id]->name = $cacheItem->cat_main_name;
                $this->categoryTree[$cacheItem->cat_main_id]->children = [];
            }
        }

        if ($cacheItem->cat_main_id && $cacheItem->cat_sub_id) {
            $found = FALSE;
            foreach ($this->categoryTree as $mainCatId => $mainCat) {
                if (array_key_exists($cacheItem->cat_sub_id, $mainCat->children)) {
                    $found = TRUE;
                    if ($mainCatId != $cacheItem->cat_main_id) {
                        //$this->log("BAD TREE DEF!: " . $cacheItem->id . ": " . $mainCatId . " != " . $cacheItem->cat_main_id);
                    }
                    break;
                }
            }
            if (!$found) {
                $this->categoryTree[$cacheItem->cat_main_id]->children[$cacheItem->cat_sub_id] = new \stdClass();
                $this->categoryTree[$cacheItem->cat_main_id]->children[$cacheItem->cat_sub_id]->name = $cacheItem->cat_sub_name;
            }
        }
    }


    /**
     * @param \stdClass $cacheItem
     * @param \stdClass $remoteItem
     */
    protected function storeCrmIdForCachedProduct($cacheItem, $remoteItem) {
        if ($remoteItem) {
            $cacheUpdateItem = new \stdClass();
            $cacheUpdateItem->id = $cacheItem->id;
            if (isset($remoteItem->updateFailure) && $remoteItem->updateFailure) {
                //we must remove crm_id and reset crm_last_update_time_c on $cacheItem
                $cacheUpdateItem->crm_id = NULL;
                $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
                $cacheUpdateItem->crm_last_update_time_c = $oldDate->format("c");
            }
            else {
                $remoteItemIdList = $remoteItem->ids;
                $remoteItemId = $remoteItemIdList[0];
                $cacheUpdateItem->crm_id = $remoteItemId;
                $now = new \DateTime();
                $cacheUpdateItem->crm_last_update_time_c = $now->format("c");
            }
            $this->productCache->updateItem($cacheUpdateItem);
        }
    }

    //-------------------------------------------------------------------------------------------------------------CACHE


    /**
     *
     * @todo: aggiungere: prodotto esaurito
     *
     *
     * @param \stdClass $localItem
     * @throws \Exception
     */
    protected function saveProductInCache($localItem) {
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
            'id' => $localItem->id,
        ];
        $candidates = $this->productCache->loadItems($filter);
        if ($candidates && count($candidates)) {
            if (count($candidates) > 1) {
                throw new \Exception("Multiple Products found for db/id!");
            }
            $cachedItem = $candidates[0];
            $updateItem = clone($localItem);
            $updateItem->metodo_last_update_time_c = $cachedItem->metodo_last_update_time_c;
            $operation = 'update';
        }

        //not there - create new
        if (!$cachedItem) {
            $updateItem = $this->generateNewProductObject($localItem);
            $operation = 'insert';
        }

        $metodoLastUpdateTime = \DateTime::createFromFormat('Y-m-d H:i:s.u', $localItem->metodo_last_update_time_c);
        $updateItemLastUpdateTime = new \DateTime($updateItem->metodo_last_update_time_c);
        if ($metodoLastUpdateTime > $updateItemLastUpdateTime) {
            $updateItem->metodo_last_update_time_c = $metodoLastUpdateTime->format("c");
        }
        else {
            $operation = "skip";
        }

        //LOG
        if ($operation != "skip") {
            $this->log(
                "-----------------------------------------------------------------------------------------"
                . $this->counters["cache"]["index"]
            );
            $this->log("[" . $itemDb . "][" . json_encode($operation) . "]:");
            //$this->log("LOCAL: " . json_encode($localItem));
            //$this->log("CACHED: " . json_encode($cachedItem));
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
                $this->productCache->addItem($updateItem);
                break;
            case "update":
                $this->productCache->updateItem($updateItem);
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
    protected function generateNewProductObject($localItem) {
        $offer = clone($localItem);
        $oldDate = \DateTime::createFromFormat('Y-m-d H:i:s', "1970-01-01 00:00:00");
        $offer->metodo_last_update_time_c = $oldDate->format("c");
        $offer->crm_last_update_time_c = $oldDate->format("c");
        return $offer;
    }


    /**
     * @param string $database IMP|MEKIT
     * @return bool|\stdClass
     */
    protected function getNextProduct($database) {
        if (!$this->localItemStatement) {
            $db = Configuration::getDatabaseConnection("SERVER2K8");
            $sql = "SELECT
                P.CodiceArticolo AS id,
                P.CodiceGruppo AS cat_main_id,
                P.DescrizioneGruppo AS cat_main_name,
                P.CodiceCategoria AS cat_sub_id,
                P.DescrizioneCategoria AS cat_sub_name,
                P.DescrizioneArticolo AS description,
                P.Um AS measurement_unit_c,
                P.Giacenza AS stock_c,
                P.Listino42 AS price,
                P.Listino9998 AS cost,
                P.Listino9997 AS price_lst_9997_c,
                P.Listino10000 AS price_lst_10000_c,
                P.VendutoUltimi120gg AS sold_last_120_days_c,
                P.DATAMODIFICA AS metodo_last_update_time_c
                FROM [${database}].[dbo].[Sog_VistaArticoliMagazzinoUltimi120gg] AS P
                WHERE P.CodiceCategoria IS NOT NULL AND P.CodiceCategoria <> '0'
                ORDER BY P.CodiceArticolo;
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
     * @param string $numberString
     * @return string
     */
    protected function fixCurrency($numberString) {
        $numberString = ($numberString ? $numberString : '0');
        $numberString = str_replace('.', ',', $numberString);
        return $numberString;
    }
}