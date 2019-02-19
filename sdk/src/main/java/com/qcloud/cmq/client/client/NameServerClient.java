package com.qcloud.cmq.client.client;

import com.qcloud.cmq.client.http.Json.JSONArray;
import com.qcloud.cmq.client.http.Json.JSONObject;
import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.common.ResponseCode;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;
import com.qcloud.cmq.client.http.HttpClient;

import java.util.*;

public class NameServerClient {

    protected HttpClient client;

    NameServerClient(String endpoint, ClientConfig config) {
        this.client = new HttpClient(endpoint, "/v2/index.php",
                config.getSecretId(), config.getSecretKey(), "GET", config.getSignMethod());
    }

    List<String> fetchQueueRoute(String queue) throws MQClientException, MQServerException {
        TreeMap<String, String> param = new TreeMap<String, String>();
        param.put("queueName", queue);
        String result = this.client.call("QueryQueueRoute", param);
        JSONObject jsonObj = new JSONObject(result);
        int code = jsonObj.getInt("code");
        if (code ==  ResponseCode.SUCCESS) {
            return parseBroker(jsonObj);
        } else {
            throw new MQServerException(code, jsonObj.getString("message"));
        }
    }

    List<String> fetchTopicRoute(String topic) throws MQClientException, MQServerException {
        TreeMap<String, String> param = new TreeMap<String, String>();
        param.put("topicName", topic);
        String result = this.client.call("QueryTopicRoute", param);
        JSONObject jsonObj = new JSONObject(result);
        int code = jsonObj.getInt("code");
        if (code ==  ResponseCode.SUCCESS) {
            return parseBroker(jsonObj);
        } else {
            throw new MQServerException(code, jsonObj.getString("message"));
        }
    }

    private static List<String> parseBroker(JSONObject jsonObject) {
        List<String> brokerInfo = new ArrayList<String>();
        JSONArray jsonArray = jsonObject.getJSONArray("addr");
        for (int i=0; i<jsonArray.length(); i++) {
            brokerInfo.add(jsonArray.getString(i));
        }
        Collections.shuffle(brokerInfo);
        return brokerInfo;
    }

}
