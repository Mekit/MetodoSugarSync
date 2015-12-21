<?php
/**
 * Created by Adam Jakab.
 * Date: 01/12/15
 * Time: 12.45
 */

namespace Mekit\Sync\SugarCrm\Rest;


use Mekit\Console\Configuration;

class SugarCrmRest {
    /** @var  \stdClass */
    private $authToken;


    public function __construct() {
    }

    /**
     * @param string     $urlSuffix
     * @param string     $type
     * @param array      $arguments
     * @param bool|TRUE  $encodeData
     * @param bool|FALSE $returnHeaders
     * @return \stdClass
     * @throws SugarCrmRestException
     */
    public function comunicate($urlSuffix, $type, $arguments = array(), $encodeData = TRUE, $returnHeaders = FALSE) {
        $cfg = Configuration::getConfiguration();
        $this->checkAuthToken();
        $url = $cfg["sugarcrm"]["url"] . $urlSuffix;
        $answer = $this->call($url, $this->authToken->access_token, $type, $arguments, $encodeData, $returnHeaders);
        if ($answer && isset($answer->error)) {
            $this->authToken = FALSE;
            $authTokenFilePath = $cfg["global"]["temporary_path"] . '/token.ser';
            if (file_exists($authTokenFilePath)) {
                unlink($authTokenFilePath);
            }
            throw new SugarCrmRestException(
                strtoupper($answer->error)
                . (isset($answer->error_message) ? ": " . $answer->error_message : '')
            );
        }
        return $answer;
    }


    /**
     * @param \stdClass $token
     * @return bool
     */
    protected function checkIfTokenIsValid($token) {
        $answer = FALSE;
        /** @var \DateTime $tokenDate */
        $tokenDate = $token->timestamp;
        $tokenExpiryDate = $tokenDate->add(new \DateInterval('PT' . ($token->expires_in - 0) . "S"));
        $now = new \DateTime();
        return ($tokenExpiryDate > $now);
    }
    /**
     * @throws SugarCrmRestException
     */
    protected function checkAuthToken() {
        if ($this->authToken && !$this->checkIfTokenIsValid($this->authToken)) {
            $this->authToken = FALSE;
        }

        if(!$this->authToken) {
            $cfg = Configuration::getConfiguration();

            $authTokenFilePath = $cfg["global"]["temporary_path"] . '/token.ser';
            if(file_exists($authTokenFilePath)) {
                $token = unserialize(file_get_contents($authTokenFilePath));
                if ($this->checkIfTokenIsValid($token)) {
                    $this->authToken = $token;
                } else {
                    unlink($authTokenFilePath);
                }
            }

            if(!file_exists($authTokenFilePath)) {
                $baseUrl = $cfg["sugarcrm"]["url"];
                $username = $cfg["sugarcrm"]["username"];
                $password = $cfg["sugarcrm"]["password"];
                $clientid = $cfg["sugarcrm"]["consumer_key"];
                $clientsecret = $cfg["sugarcrm"]["consumer_secret"];
                $loginUrl = $baseUrl . "/oauth2/token";
                $arguments = array(
                    "grant_type" =>     "password",
                    "client_id" =>      $clientid,
                    "client_secret" =>  $clientsecret,
                    "username" =>       $username,
                    "password" =>       $password,
                    "platform" =>       "base"
                );
                /** @var \stdClass $token */
                $token = $this->call($loginUrl, '', 'POST', $arguments);
                if(isset($token->error)) {
                    throw new SugarCrmRestException(
                        strtoupper($token->error)
                        . (isset($token->error_message) ? ": " . $token->error_message : '')
                    );
                }
                $this->authToken = $token;
                $token->timestamp = new \DateTime();
                file_put_contents($authTokenFilePath, serialize($token));
            }
        }
    }

    protected function call(
        $url,
        $oauthtoken = '',
        $type = 'GET',
        $arguments = array(),
        $encodeData = TRUE,
        $returnHeaders = FALSE
    ) {
        $type = strtoupper($type);

        if ($type == 'GET') {
            $url .= "?" . http_build_query($arguments);
        }

        $curl_request = curl_init($url);

        if ($type == 'POST') {
            curl_setopt($curl_request, CURLOPT_POST, 1);
        }
        elseif ($type == 'PUT') {
            curl_setopt($curl_request, CURLOPT_CUSTOMREQUEST, "PUT");
        }
        elseif ($type == 'DELETE') {
            curl_setopt($curl_request, CURLOPT_CUSTOMREQUEST, "DELETE");
        }

        curl_setopt($curl_request, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_0);
        curl_setopt($curl_request, CURLOPT_HEADER, $returnHeaders);
        curl_setopt($curl_request, CURLOPT_SSL_VERIFYPEER, 0);
        curl_setopt($curl_request, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($curl_request, CURLOPT_FOLLOWLOCATION, 0);
        curl_setopt($curl_request, CURLOPT_CONNECTTIMEOUT, 5);//timeout in seconds
        curl_setopt($curl_request, CURLOPT_TIMEOUT, 10); //timeout in seconds

        if (!empty($oauthtoken)) {
            $token = array("oauth-token: {$oauthtoken}");
            curl_setopt($curl_request, CURLOPT_HTTPHEADER, $token);
        }

        if (!empty($arguments) && $type !== 'GET') {
            if ($encodeData) {
                //encode the arguments as JSON
                $arguments = json_encode($arguments);
            }
            curl_setopt($curl_request, CURLOPT_POSTFIELDS, $arguments);
        }

        $result = curl_exec($curl_request);

        if ($returnHeaders) {
            //set headers from response
            list($headers, $content) = explode("\r\n\r\n", $result, 2);
            foreach (explode("\r\n", $headers) as $header) {
                header($header);
            }

            //return the nonheader data
            return trim($content);
        }
        $curl_errno = curl_errno($curl_request);
        $curl_error = curl_error($curl_request);
        curl_close($curl_request);
        if ($curl_errno > 0) {
            throw new SugarCrmRestException($curl_error, $curl_errno);
        }

        //decode the response from JSON
        $response = json_decode($result);

        //@todo: check for errors

        return $response;
    }

}