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
     */
    public function execute($options) {
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
        foreach (["IMP"] as $database) {
            while ($localItem = $this->getNextOffer($database)) {
                if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counters["cache"]["index"] >= $FORCE_LIMIT) {
                    break;
                }
                $this->counters["cache"]["index"]++;
                $this->saveOfferInCache($localItem);
            }
        }
    }


    protected function updateRemoteFromCache() {
        $FORCE_LIMIT = FALSE;
        $this->log("updating remote(offers)...");
        $this->productCache->resetItemWalker();
        $this->counters["remote"]["index"] = 0;
        $categoryTree = [];
        while ($cacheItem = $this->productCache->getNextItem()) {
            if (isset($FORCE_LIMIT) && $FORCE_LIMIT && $this->counters["remote"]["index"] >= $FORCE_LIMIT) {
                $this->productCache->resetItemWalker();
                break;
            }
            $this->counters["remote"]["index"]++;
            $categoryTree = $this->buildCategoryTree($categoryTree, $cacheItem);
        }
        $this->registerRemoteProductCategories($categoryTree);


        //$remoteItem = $this->saveOfferOnRemote($cacheItem);
        //$this->storeCrmIdForCachedOffer($cacheItem, $remoteItem);
    }


    protected function registerRemoteProductCategories($categoryTree) {
        $this->log(json_encode($categoryTree));

        //PARENT CATEGORIES
        foreach ($categoryTree as $mainCatId => &$mainCat) {
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
                        $this->log("REMOTE RESULT: " . json_encode($result));
                    } catch(SugarCrmRestException $e) {
                        //go ahead with false silently
                        $this->log("REMOTE ERROR!!! - " . $e->getMessage());
                    }
                }
                else {
                    //already registered
                    $mainCat->crm_id = $crm_id;
                }
            } catch(\Exception $e) {
                $this->log("CANNOT LOAD ID FROM CRM - UPDATE WILL BE SKIPPED FOR: " . $mainCat->name);
            }
        }

        //CHILD CATEGORIES
        foreach ($categoryTree as $mainCatId => &$mainCat) {
            if (isset($mainCat->crm_id)) {
                $mainCatCrmId = $mainCat->crm_id;
                foreach ($mainCat->children as $childCatId => &$childCat) {
                    try {
                        $crm_id = $this->loadRemoteProductCategoryId($childCatId);
                        if ($crm_id === FALSE) {
                            $data = new \stdClass();
                            $data->name = $childCat->name;
                            $data->metodo_category_code_c = $childCatId;
                            $data->parent_categoty_id = $mainCatCrmId;
                            $data->is_parent = 0;
                            $arguments = [
                                'module_name' => 'AOS_Product_Categories',
                                'name_value_list' => $this->sugarCrmRest->createNameValueListFromObject($data),
                            ];
                            try {
                                $result = $this->sugarCrmRest->comunicate('set_entries', $arguments);
                                $this->log("REMOTE RESULT: " . json_encode($result));
                            } catch(SugarCrmRestException $e) {
                                //go ahead with false silently
                                $this->log("REMOTE ERROR!!! - " . $e->getMessage());
                            }
                        }
                        else {
                            //already registered
                            $childCat->crm_id = $crm_id;
                        }
                    } catch(\Exception $e) {
                        $this->log("CANNOT LOAD CATEGORY ID FROM CRM - UPDATE WILL BE SKIPPED FOR: " . $childCat->name);
                    }
                }
            }
        }

        print_r($categoryTree);

    }


    /**
     * AOS_Product_Categories
     * They would be recreated all over again
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
     * @param array     $categoryTree
     * @param \stdClass $cacheItem
     * @return array
     */
    protected function buildCategoryTree($categoryTree, $cacheItem) {
        if ($cacheItem->cat_main_id) {
            if (!array_key_exists($cacheItem->cat_main_id, $categoryTree)) {
                $categoryTree[$cacheItem->cat_main_id] = new \stdClass();
                $categoryTree[$cacheItem->cat_main_id]->name = $cacheItem->cat_main_name;
                $categoryTree[$cacheItem->cat_main_id]->children = [];
            }
        }

        if ($cacheItem->cat_main_id && $cacheItem->cat_sub_id) {
            $found = FALSE;
            foreach ($categoryTree as $mainCatId => $mainCat) {
                if (array_key_exists($cacheItem->cat_sub_id, $mainCat->children)) {
                    $found = TRUE;
                    if ($mainCatId != $cacheItem->cat_main_id) {
                        //$this->log("BAD TREE DEF!: " . $cacheItem->id . ": " . $mainCatId . " != " . $cacheItem->cat_main_id);
                    }
                    break;
                }
            }
            if (!$found) {
                $categoryTree[$cacheItem->cat_main_id]->children[$cacheItem->cat_sub_id] = new \stdClass();
                $categoryTree[$cacheItem->cat_main_id]->children[$cacheItem->cat_sub_id]->name = $cacheItem->cat_sub_name;
            }
        }
        return $categoryTree;
    }


    /**
     * @param \stdClass $cacheItem
     * @param \stdClass $remoteItem
     */
    protected function storeCrmIdForCachedOffer($cacheItem, $remoteItem) {
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
                $cacheUpdateItem->crm_id = $remoteItem->id;
                $now = new \DateTime();
                $cacheUpdateItem->crm_last_update_time_c = $now->format("c");
            }
            $this->productCache->updateItem($cacheUpdateItem);
        }
    }

    //-------------------------------------------------------------------------------------------------------------CACHE


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


        $this->log(
            "-----------------------------------------------------------------------------------------"
            . $this->counters["cache"]["index"]
        );

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
    protected function getNextOffer($database) {
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
}