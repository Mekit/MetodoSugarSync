<?php
namespace Mekit\DbCache;

class CacheDb extends SqliteDb {
    /** @var  \PDOStatement */
    private $filesWalker;

    /**
     * @param string $dataIdentifier
     * @param callable $logger
     */
    public function __construct($dataIdentifier, $logger) {
        parent::__construct($dataIdentifier, $logger);
        $this->setupDatabase();
    }

    /**
     * @param array $filter
     * @return bool|mixed
     */
    public function loadItem($filter) {
        $answer = FALSE;
        if (count($filter)) {
            $query = "SELECT * FROM " . $this->dataIdentifier . " WHERE";//file_path_md5 = :file_path_md5";
            $filterIndex = 1;
            $maxFilters = count($filter);
            foreach (array_keys($filter) as $filterParam) {
                $query .= " " . $filterParam . ' = :' . $filterParam . ($filterIndex < $maxFilters ? " AND" : "");
                $filterIndex++;
            }
            $stmt = $this->db->prepare($query);
            foreach ($filter as $filterParam => $filterValue) {
                $stmt->bindParam(':' . $filterParam, $filterValue, \PDO::PARAM_STR);
            }
            if ($stmt->execute()) {
                $answer = $stmt->fetch(\PDO::FETCH_ASSOC);
                $this->log("loaded cache item: " . json_encode($answer));
            }
        }

        return $answer;
    }

    public function saveItem($item) {

    }


    /**
     * @param bool|TRUE $forceRecreate
     */
    protected function setupDatabase($forceRecreate = TRUE) {
        if ($forceRecreate) {
            $this->db->exec("DROP TABLE IF EXISTS " . $this->dataIdentifier);
            $this->log("Cache table($this->dataIdentifier) dropped.");
        }
        $statement = $this->db->prepare(
            "SELECT COUNT(*) AS HASTABLE FROM sqlite_master WHERE type='table' AND name='" . $this->dataIdentifier . "'"
        );
        $statement->execute();
        $tabletest = $statement->fetchObject();
        $hasTable = $tabletest->HASTABLE == 1;
        if (!$hasTable) {
            $this->log("creating cache table($this->dataIdentifier)...");
            $sql = "CREATE TABLE " . $this->dataIdentifier . " ("
                   . "id TEXT NOT NULL"
                   . ", metodo_client_code_imp_c TEXT"
                   . ", metodo_supplier_code_imp_c TEXT"
                   . ", metodo_client_code_mekit_c TEXT"
                   . ", metodo_supplier_code_mekit_c TEXT"
                   . ", partita_iva_c TEXT"
                   . ", codice_fiscale_c TEXT"
                   . ", metodo_last_update_time_c INTEGER NOT NULL"
                   . ")";
            $this->db->exec($sql);
            $this->db->exec("CREATE UNIQUE INDEX CRMID ON " . $this->dataIdentifier . " (id ASC)");
            $this->db->exec("CREATE INDEX IMP_C ON " . $this->dataIdentifier . " (metodo_client_code_imp_c ASC)");
            $this->db->exec("CREATE INDEX IMP_F ON " . $this->dataIdentifier . " (metodo_supplier_code_imp_c ASC)");
            $this->db->exec("CREATE INDEX MKT_C ON " . $this->dataIdentifier . " (metodo_client_code_mekit_c ASC)");
            $this->db->exec("CREATE INDEX MKT_F ON " . $this->dataIdentifier . " (metodo_supplier_code_mekit_c ASC)");
            $this->db->exec("CREATE INDEX P_IVA ON " . $this->dataIdentifier . " (partita_iva_c ASC)");
            $this->db->exec("CREATE INDEX CFIS ON " . $this->dataIdentifier . " (codice_fiscale_c ASC)");
        }
    }
}

//
///**
// * @param string $filePath
// * @return bool|mixed
// */
//public function getFile($filePath) {
//    $answer = false;
//    $query = "SELECT * FROM files WHERE file_path_md5 = :file_path_md5";
//    $stmt = $this->db->prepare($query);
//    $stmt->bindParam(':file_path_md5', md5($filePath), \PDO::PARAM_STR);
//    if($stmt->execute()) {
//        $answer = $stmt->fetch(\PDO::FETCH_ASSOC);
//        if(!is_array($answer) || $answer["file_path"] != $filePath) {
//            //echo ("NOT FOUND: '" . $filePath . "' - " . md5($filePath) . "\n");
//            $answer = false;
//        }
//    }
//    return $answer;
//}
//
///**
// * Returns the next row on each call
// * @param boolean $reset
// * @param string|null $status
// * @return bool|mixed
// */
//public function getNextFile($reset=false, $status=null) {
//    if(!$this->filesWalker || $reset) {
//        $query = "SELECT * FROM files";
//        if($status) {
//            $query .= " WHERE file_status = :file_status";
//        }
//        $this->filesWalker = $this->db->prepare($query);
//        if($status) {
//            $this->filesWalker->bindParam(':file_status', $status, \PDO::PARAM_STR);
//        }
//        $this->filesWalker->execute();
//    }
//    $answer = $this->filesWalker->fetch(\PDO::FETCH_ASSOC);
//    return is_array($answer) ? $answer : false;
//}
//
///**
// * Returns the next row on each call - ONLY FILES WITH status not OK
// * @param boolean $reset
// * @return bool|mixed
// */
//public function getNextScannableFile($reset=false) {
//    if(!$this->filesWalker || $reset) {
//        $query = "SELECT * FROM files"
//                 . " WHERE file_status <> :file_status";
//        $this->filesWalker = $this->db->prepare($query);
//        $status = "OK";
//        $this->filesWalker->bindParam(':file_status', $status, \PDO::PARAM_STR);
//        $this->filesWalker->execute();
//    }
//    $answer = $this->filesWalker->fetch(\PDO::FETCH_ASSOC);
//    return is_array($answer) ? $answer : false;
//}
//
///**
// * @param string $filePath
// * @return bool
// */
//public function addFile($filePath) {
//    $file = $this->getFile($filePath);
//    if(!$file) {
//        try {
//            $query = "INSERT INTO files (file_path_md5, file_path, file_md5, file_status, check_time) VALUES"
//                     ." (:file_path_md5, :file_path, :file_md5, :file_status, :check_time)";
//            $stmt = $this->db->prepare($query);
//            $stmt->bindParam(':file_path_md5', md5($filePath));
//            $stmt->bindParam(':file_path', $filePath);
//            $stmt->bindParam(':file_md5', md5_file($filePath));
//            $file_status = "UNCHECKED";
//            $stmt->bindParam(':file_status', $file_status);
//            $check_time = 0;
//            $stmt->bindParam(':check_time', $check_time);
//            $stmt->execute();
//            return true;
//        } catch (\PDOException $e) {
//            return false;
//        }
//    }
//    return true;
//}
//
///**
// * @param string $filePath
// * @param string $fileStatus
// * @return bool
// */
//public function updateFile($filePath, $fileStatus){
//    $file = $this->getFile($filePath);
//    if(!$file) {
//        return false;
//    }
//    try {
//        $query = "UPDATE files SET"
//                 ." file_md5 = :file_md5,"
//                 ." file_status = :file_status,"
//                 ." check_time = :check_time"
//                 ." WHERE file_path_md5 = :file_path_md5";
//        $stmt = $this->db->prepare($query);
//        $stmt->bindParam(':file_path_md5', md5($filePath));
//        $stmt->bindParam(':file_md5', md5_file($filePath));
//        $stmt->bindParam(':file_status', $fileStatus);
//        $check_time = time();
//        $stmt->bindParam(':check_time', $check_time);
//        $stmt->execute();
//        return true;
//    } catch (\PDOException $e) {
//        return false;
//    }
//}
//
///**
// * @param string $filePath
// * @return bool
// */
//public function removeFile($filePath) {
//    try {
//        $query = "DELETE FROM files WHERE file_path_md5 = :file_path_md5";
//        $stmt = $this->db->prepare($query);
//        $stmt->bindParam(':file_path_md5', md5($filePath));
//        return $stmt->execute();
//    } catch (\PDOException $e) {
//        return false;
//    }
//}
//
//public function beginTransaction() {
//    $this->db->beginTransaction();
//}
//
//public function commitTransaction() {
//    $this->db->commit();
//}
//
///**
// * @return mixed
// */
//public function getCount() {
//    $stmt = $this->db->query('SELECT COUNT(*) AS count FROM files');
//    $res = $stmt->fetch();
//    return ((int)$res["count"]);
//}