<?php
/**
 * Created by Adam Jakab.
 * Date: 01/02/16
 * Time: 17.42
 */
$url = "http://crm2.ingrossomaterialipulizia.it/service/v4_1/rest.php";
$username = "tmpadmin";
$password = "tmpadmin";

//function to make cURL request
function call($method, $parameters, $url) {
    ob_start();
    $curl_request = curl_init();

    curl_setopt($curl_request, CURLOPT_URL, $url);
    curl_setopt($curl_request, CURLOPT_POST, 1);
    curl_setopt($curl_request, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_0);
    curl_setopt($curl_request, CURLOPT_HEADER, 1);
    curl_setopt($curl_request, CURLOPT_SSL_VERIFYPEER, 0);
    curl_setopt($curl_request, CURLOPT_RETURNTRANSFER, 1);
    curl_setopt($curl_request, CURLOPT_FOLLOWLOCATION, 0);

    $jsonEncodedData = json_encode($parameters);

    $post = array(
        "method" => $method,
        "input_type" => "JSON",
        "response_type" => "JSON",
        "rest_data" => $jsonEncodedData
    );

    curl_setopt($curl_request, CURLOPT_POSTFIELDS, $post);
    $result = curl_exec($curl_request);
    curl_close($curl_request);

    $result = explode("\r\n\r\n", $result, 2);
    $response = json_decode($result[1]);
    ob_end_flush();

    return $response;
}

//login ------------------------------
$login_parameters = array(
    "user_auth" => array(
        "user_name" => $username,
        "password" => md5($password),
        "version" => "1"
    ),
    "application_name" => "RestTest",
    "name_value_list" => array(),
);

$login_result = call("login", $login_parameters, $url);

if (!isset($login_result->id)) {
    print_r($login_result);
    exit();
}

//get session id
$session_id = $login_result->id;


//-----------------------------------------------------------
//get list of records --------------------------------
$get_entry_list_parameters = array(

    //session id
    'session' => $session_id,
    //The name of the module from which to retrieve records
    'module_name' => 'Accounts',
    //The SQL WHERE clause without the word "where".
    'query' => "accounts.industry = 9",
    //The SQL ORDER BY clause without the phrase "order by".
    'order_by' => "",
    //The record offset from which to start.
    'offset' => '0',
    //Optional. A list of fields to include in the results.
    'select_fields' => array(
        'id',
        'name',
        'industry'
    ),
    /*
    A list of link names and the fields to be returned for each link name.
    Example: 'link_name_to_fields_array' => array(array('name' => 'email_addresses', 'value' => array('id', 'email_address', 'opt_out', 'primary_address')))
    */
    'link_name_to_fields_array' => array(),
    //The maximum number of results to return.
    'max_results' => '1',
    //To exclude deleted records
    'deleted' => '0',
    //If only records marked as favorites should be returned.
    'Favorites' => FALSE,
);
//$get_entry_list_parameters["session"] = $session_id;

$get_entry_list_result = call('get_entry_list', $get_entry_list_parameters, $url);

echo '<pre>';
print_r($get_entry_list_result);
echo '</pre>';