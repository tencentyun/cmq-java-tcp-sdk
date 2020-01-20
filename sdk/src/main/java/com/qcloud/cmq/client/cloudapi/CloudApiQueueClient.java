package com.qcloud.cmq.client.cloudapi;

import com.qcloud.cmq.client.cloudapi.entity.CmqQueue;

import java.util.List;

/**
 * @author: feynmanlin
 * @date: 2020/1/14 7:25 下午
 */
public interface CloudApiQueueClient {

    String createQueue(String queueName, QueueMeta meta);

    int countQueue(String searchWord, int offset, int limit);

    List<CmqQueue> describeQueue(String searchWord, int offset, int limit);

    void deleteQueue(String queueName);

    QueueMeta getQueueAttributes(String queueName);
}
