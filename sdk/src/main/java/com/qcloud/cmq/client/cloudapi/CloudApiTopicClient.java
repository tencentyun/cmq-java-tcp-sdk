package com.qcloud.cmq.client.cloudapi;

/**
 * @author: feynmanlin
 * @date: 2020/1/15 11:23 上午
 */
public interface CloudApiTopicClient {

    String createSubscribe(SubscribeConfig subscribeConfig);

    String deleteSubscribe(String topicName, String subscriptionName);
}
