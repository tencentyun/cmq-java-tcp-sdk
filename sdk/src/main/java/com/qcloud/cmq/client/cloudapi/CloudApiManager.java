package com.qcloud.cmq.client.cloudapi;

import com.qcloud.cmq.client.client.CloudApiQueueClientV2;
import com.qcloud.cmq.client.client.CloudApiTopicClientV2;
import com.qcloud.cmq.client.cloudapi.entity.CmqQueue;
import com.qcloud.cmq.client.common.AssertUtil;
import com.qcloud.cmq.client.common.CloudApiClientConfig;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.http.CloudApiHttpClient;

import java.util.List;

/**
 * @author: feynmanlin
 * @date: 2020/1/14 4:45 下午
 */
public class CloudApiManager extends CloudApiClientConfig {

    private volatile CloudApiQueueClient cloudApiQueueClient;
    private volatile CloudApiTopicClient cloudApiTopicClient;

    public void start() throws MQClientException {
        initClient();
    }

    private void initClient() {
        if (cloudApiTopicClient == null || cloudApiQueueClient == null) {
            synchronized (this) {
                if (cloudApiTopicClient == null || cloudApiQueueClient == null) {
                    CloudApiHttpClient cloudApiHttpClient = new CloudApiHttpClient(this);
                    cloudApiTopicClient = new CloudApiTopicClientV2(cloudApiHttpClient);
                    cloudApiQueueClient = new CloudApiQueueClientV2(cloudApiHttpClient);
                }
            }
        }
    }

    public String createQueue(String queueName) {
        return cloudApiQueueClient.createQueue(queueName, new QueueMeta());
    }

    public String createQueue(String queueName, QueueMeta queueMeta) {
        AssertUtil.assertParamNotNull(queueMeta, "QueueMeta is null");
        return cloudApiQueueClient.createQueue(queueName, queueMeta);
    }

    public int countQueue(String searchWord, int offset, int limit) {
        return cloudApiQueueClient.countQueue(searchWord, offset, limit);
    }

    public List<CmqQueue> describeQueue(String searchWord, int offset, int limit) {
        return cloudApiQueueClient.describeQueue(searchWord, offset, limit);
    }

    public String createQueueAndSubscribe(String queueName, String topicName, String subscribeName) {
        return createQueueAndSubscribe(queueName, new QueueMeta(), topicName, subscribeName);
    }

    public String createSubscribe(SubscribeConfig subscribeConfig) {
        return cloudApiTopicClient.createSubscribe(subscribeConfig);
    }

    public void deleteSubscribe(String topicName, String subscriptionName) {
        cloudApiTopicClient.deleteSubscribe(topicName, subscriptionName);
    }

    public String createQueueAndSubscribe(String queueName, QueueMeta queueMeta, String topicName, String subscribeName) {
        String queueId = cloudApiQueueClient.createQueue(queueName, queueMeta);

        SubscribeConfig subscribeConfig = new SubscribeConfig();
        subscribeConfig.setTopicName(topicName);
        subscribeConfig.setProtocol("queue");
        subscribeConfig.setSubscriptionName(subscribeName);
        subscribeConfig.setEndpoint(queueName);

        cloudApiTopicClient.createSubscribe(subscribeConfig);

        return queueId;
    }


    public QueueMeta describeQueueAttributes(String queueName) {
        return cloudApiQueueClient.getQueueAttributes(queueName);
    }

    public void deleteQueue(String queueName) {
        cloudApiQueueClient.deleteQueue(queueName);
    }


}
