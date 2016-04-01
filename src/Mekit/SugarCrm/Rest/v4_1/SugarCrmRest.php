<?php
/**
 * Created by Adam Jakab.
 * Date: 11/02/16
 * Time: 11.45
 */

namespace Mekit\SugarCrm\Rest\v4_1;

use Mekit\Console\Configuration;

class SugarCrmRest {
    /** @var string */
    private $sessionFileName = 'session_v4_1.sess';

    /** @var string */
    private $sessionId;

    /**
     * @param string $method
     * @param array  $arguments
     * @return \stdClass|bool
     * @throws SugarCrmRestException
     */
    public function comunicate($method, $arguments) {
        $result = FALSE;
        $this->checkCreateSession();
        $cfg = Configuration::getConfiguration();
        $url = $cfg["suitecrm"]["url"];
        try {
            $result = $this->call($method, $arguments, $url);
        } catch(SugarCrmRestException $e) {
            if ($e->getCode() == 102) {
                //INVALID SESSION - Remove Session file
                $sessionFilePath = $cfg["global"]["temporary_path"] . '/' . $this->sessionFileName;
                unlink($sessionFilePath);
                $this->sessionId = FALSE;
            }
            throw $e;
        }
        return $result;
    }

    /**
     * @param \stdClass $obj
     * @return array
     */
    public function createNameValueListFromObject($obj) {
        $element = [];
        foreach (get_object_vars($obj) as $key => $val) {
            $element[] = [
                "name" => $key,
                "value" => $val
            ];
        }
        $answer = [$element];
        return $answer;
    }

    /**
     * @param \stdClass $entryListItem
     * @param \stdClass $entryListItem
     * @return \stdClass
     */
    public function getNameValueListFromEntyListItem($entryListItem, $relationshipListItem = NULL) {
        $answer = new \stdClass();
        if (isset($entryListItem->name_value_list) && get_object_vars($entryListItem->name_value_list)) {
            /** @var \stdClass $valueObject */
            foreach (get_object_vars($entryListItem->name_value_list) as $valueObject) {
                $k = $valueObject->name;
                $v = $valueObject->value;
                $answer->$k = $v;
            }
        }

        if (isset($relationshipListItem->link_list) && count($relationshipListItem->link_list)) {
            /** @var \stdClass $linkItem */
            foreach ($relationshipListItem->link_list as $linkItem) {
                if (count($linkItem->records)) {
                    $linkItemName = $linkItem->name;
                    $linkItemValues = [];
                    /** @var \stdClass $record */
                    foreach ($linkItem->records as $record) {
                        if (isset($record->link_value)) {
                            $linkItemElement = new \stdClass();
                            foreach (get_object_vars($record->link_value) as $valueObjectKey => $valueObjectValue) {
                                $k = $valueObjectValue->name;
                                $v = $valueObjectValue->value;
                                $linkItemElement->$k = $v;
                            }
                            $linkItemValues[] = $linkItemElement;
                        }
                    }
                    if (count($linkItemValues)) {
                        $answer->$linkItemName = $linkItemValues;
                    }
                }
            }
        }

        return $answer;
    }

    protected function checkCreateSession() {
        if ($this->sessionId && !$this->checkIfSessionIsValid($this->sessionId)) {
            $this->sessionId = FALSE;
        }

        if (!$this->sessionId) {
            $cfg = Configuration::getConfiguration();

            $sessionFilePath = $cfg["global"]["temporary_path"] . '/' . $this->sessionFileName;
            if (file_exists($sessionFilePath)) {
                /** @var \stdClass $sessionData */
                $sessionData = @unserialize(file_get_contents($sessionFilePath));
                if ($sessionData && $this->checkIfSessionIsValid($sessionData->id)) {
                    $this->sessionId = $sessionData->id;
                }
                else {
                    unlink($sessionFilePath);
                }
            }

            if (!file_exists($sessionFilePath)) {
                $baseUrl = $cfg["suitecrm"]["url"];
                $username = $cfg["suitecrm"]["username"];
                $password = $cfg["suitecrm"]["password"];
                $application = $cfg["suitecrm"]["application"];
                $arguments = [
                    "user_auth" => [
                        "user_name" => $username,
                        "password" => md5($password),
                        "version" => "1"
                    ],
                    "application_name" => $application,
                    "name_value_list" => [],
                ];

                /** @var \stdClass $sessionData */
                $sessionData = $this->call("login", $arguments, $baseUrl);
                //echo "SessionData: " . json_encode($sessionData);

                if (!$sessionData || !isset($sessionData->id) || empty($sessionData->id)) {
                    throw new SugarCrmRestException("Unable to get session ID!");
                }
                $this->sessionId = $sessionData->id;
                file_put_contents($sessionFilePath, serialize($sessionData));
            }
        }
    }

    /**
     * @param string $sessionId
     * @return bool
     */
    protected function checkIfSessionIsValid($sessionId) {
        $answer = TRUE;
        return $answer;
    }

    /**
     * @param string $method
     * @param array  $arguments
     * @param string $url
     * @return mixed
     * @throws SugarCrmRestException
     */
    protected function call($method, $arguments, $url) {
        ob_start();
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_POST, 1);
        curl_setopt($ch, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_0);
        curl_setopt($ch, CURLOPT_HEADER, 0);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 0);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, 0);
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 5);//timeout in seconds
        curl_setopt($ch, CURLOPT_TIMEOUT, 10); //timeout in seconds

        $restArguments = [];
        if ('login' != $method) {
            //!!!For some obscure reason session key must be the first in the array otherwise you'll get 'Invalid Session ID'
            $restArguments['session'] = $this->sessionId;
        }
        $restArguments = array_merge($restArguments, $arguments);
        $restArguments = json_encode($restArguments);

        $post = array(
            "method" => $method,
            "input_type" => "JSON",
            "response_type" => "JSON",
            "rest_data" => $restArguments
        );
        //echo "\nPOST: " . json_encode($post);

        curl_setopt($ch, CURLOPT_POSTFIELDS, $post);
        $result = curl_exec($ch);
        if (!curl_errno($ch)) {
            $info = curl_getinfo($ch);
            if ($info["http_code"] != 200) {
                throw new SugarCrmRestException(
                    "Curl response status code error(" . $info["http_code"] . ") is not 200!", 100
                );
            }
            else if ($info['content_type'] != 'application/json; charset=UTF-8') {
                echo "Response: " . $result . "\n";
                throw new SugarCrmRestException(
                    "Curl response content type error(" . $info["content_type"]
                    . ") is not 'application/json; charset=UTF-8'!", 101
                );
            }
            else if (preg_match("#\"Invalid Session ID\"#i", $result)) {
                throw new SugarCrmRestException("Invalid Session ID! Re-authenticate!", 102);
            }
            $response = json_decode($result);
        }
        else {
            throw new SugarCrmRestException("Curl execution error! " . curl_error($ch), curl_errno($ch));
        }
        curl_close($ch);
        ob_end_flush();
        return $response;
    }
}