package com.qcloud.cmq.client.client;

import com.qcloud.cmq.client.cloudapi.CloudApiTopicClient;
import com.qcloud.cmq.client.cloudapi.SubscribeConfig;
import com.qcloud.cmq.client.common.AssertUtil;
import com.qcloud.cmq.client.common.CMQTool;
import com.qcloud.cmq.client.common.ResponseCode;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.http.CloudApiHttpClient;
import com.qcloud.cmq.client.http.HttpWrapper;
import io.netty.util.internal.StringUtil;

import java.util.List;
import java.util.TreeMap;

/**
 * @author: feynmanlin
 * @date: 2020/1/14 5:12 下午
 */
public class CloudApiTopicClientV2 implements CloudApiTopicClient {

    private HttpWrapper cloudApiHttpUtil;

    public CloudApiTopicClientV2(CloudApiHttpClient cloudApiHttpClient) {
        this.cloudApiHttpUtil = cloudApiHttpClient;
    }

    @Override
    public void createSubscribe(SubscribeConfig subscribeConfig) {
        createSubscribe(subscribeConfig.getTopicName(), subscribeConfig.getSubscriptionName(), subscribeConfig.getEndpoint()
                , subscribeConfig.getProtocol(), subscribeConfig.getFilterTag(), subscribeConfig.getBindingKey(), subscribeConfig.getNotifyStrategy()
                , subscribeConfig.getNotifyContentFormat());
    }

    public void createSubscribe(String topicName, String subscriptionName, String endpoint, String protocol,
                                List<String> filterTag, List<String> bindingKey, String notifyStrategy, String notifyContentFormat) {
        if (filterTag != null && filterTag.size() > 5) {
            throw new MQClientException(ResponseCode.INVALID_REQUEST_PARAMETERS, "Invalid parameter: Tag number > 5");
        }

        TreeMap<String, String> param = new TreeMap();

        AssertUtil.assertParamNotNull(topicName, "Invalid parameter:topicName is empty");
        param.put("topicName", topicName);

        AssertUtil.assertParamNotNull(subscriptionName, "Invalid parameter:subscriptionName is empty");
        param.put("subscriptionName", subscriptionName);

        AssertUtil.assertParamNotNull(endpoint, "Invalid parameter:Endpoint is empty");
        param.put("endpoint", endpoint);

        AssertUtil.assertParamNotNull(protocol, "Invalid parameter:Protocal is empty");
        param.put("protocol", protocol);

        if (!StringUtil.isNullOrEmpty(notifyStrategy)) {
            param.put("notifyStrategy", notifyStrategy);
        }

        if (StringUtil.isNullOrEmpty(notifyContentFormat)) {
            param.put("notifyContentFormat", notifyContentFormat);
        }

        if (filterTag != null) {
            for (int i = 0; i < filterTag.size(); ++i) {
                param.put("filterTag." + (i + 1), filterTag.get(i));
            }
        }

        if (bindingKey != null) {
            for (int i = 0; i < bindingKey.size(); ++i) {
                param.put("bindingKey." + (i + 1), bindingKey.get(i));
            }
        }

        String result = cloudApiHttpUtil.call("Subscribe", param);
        CMQTool.checkResult(result);
    }

}
