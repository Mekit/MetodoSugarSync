<?php

/*
 * "symfony/console": "^2.6",
        "psr/log": "^1.0",
        "swiftmailer/swiftmailer": "^5.4",
        "symfony/yaml": "^2.6",
        "symfony/filesystem": "^2.6"
 */
$serverName = "SERVER2K8-IMP";
$username = "CrmSync";
$password = "CrmSync";

try {
  $db = new PDO("odbc:$serverName", "$username", "$password");
} catch(PDOException $exception) {
  die("Unable to open database!\n" . $exception->getMessage() . "\n");
}
echo "Successfully connected!\n";


try {
  $query = "SELECT * FROM [SogCRM_TesteDocumenti] WHERE [TipoDoc] = 'DDT'";
  $statement = $db->prepare($query);
  $statement->execute();
  $result = $statement->fetchAll(PDO::FETCH_ASSOC);
} catch(Exception $exception) {
  die("Unable to execute query!\n" . $exception->getMessage() . "\n");
}

echo "Documents: " . count($result);

print_r($result[0]);
echo "\n";