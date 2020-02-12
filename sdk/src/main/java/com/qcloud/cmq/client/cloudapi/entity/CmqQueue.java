package com.qcloud.cmq.client.cloudapi.entity;

/**
 * @author: feynmanlin
 * @date: 2020/1/17 10:24 上午
 */
public class CmqQueue {

    private String queueId;
    private String queueName;

    public String getQueueId() {
        return queueId;
    }

    public void setQueueId(String queueId) {
        this.queueId = queueId;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    @Override
    public String toString() {
        return "CmqQueue{" +
                "queueId='" + queueId + '\'' +
                ", queueName='" + queueName + '\'' +
                '}';
    }
}
