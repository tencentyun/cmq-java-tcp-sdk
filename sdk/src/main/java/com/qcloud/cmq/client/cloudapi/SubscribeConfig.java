package com.qcloud.cmq.client.cloudapi;

import java.util.List;

/**
 * @author: feynmanlin
 * @date: 2020/1/15 11:52 上午
 */
public class SubscribeConfig {
    private String topicName;
    private String subscriptionName;
    /**
     * （1）对于 HTTP，endpoint 必须以http://开头，host 可以是域名或 IP，endpoint 长度最大为500
     * （2）对于 queue，则填 queueName
     */
    private String endpoint;
    /**
     * 订阅的协议，目前支持两种协议：HTTP、queue。使用 HTTP 协议，用户需自己搭建接受消息的 Web Server。
     * 使用 queue，消息会自动推送到 CMQ queue，用户可以并发地拉取消息。
     */
    private String protocol;
    private List<String> filterTag;
    private List<String> bindingKey;
    /**
     *  非必填
     * （1）BACKOFF_RETRY，退避重试。每隔[19,29]s 重试一次，重试3次后，就把该消息丢弃，继续推送下一条消息
     * （2）EXPONENTIAL_DECAY_RETRY，指数衰退重试。每次重试的间隔是指数递增的，例如开始 1s，后面是2s，4s，8s...由于 Topic 消息的周期是一天，
     *     所以最多重试一天就把消息丢弃。默认值是 EXPONENTIAL_DECAY_RETRY
     */
    private String notifyStrategy;
    /**
     * 非必填
     * 推送内容的格式。取值：（1）JSON。（2）SIMPLIFIED，即 raw 格式。如果 protocol 是 Queue，则取值必须为 SIMPLIFIED。
     * 如果 protocol 是 HTTP，两个值均可以，默认值是 JSON。
     */
    private String notifyContentFormat;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }

    public void setSubscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public List<String> getFilterTag() {
        return filterTag;
    }

    public void setFilterTag(List<String> filterTag) {
        this.filterTag = filterTag;
    }

    public List<String> getBindingKey() {
        return bindingKey;
    }

    public void setBindingKey(List<String> bindingKey) {
        this.bindingKey = bindingKey;
    }

    public String getNotifyStrategy() {
        return notifyStrategy;
    }

    public void setNotifyStrategy(String notifyStrategy) {
        this.notifyStrategy = notifyStrategy;
    }

    public String getNotifyContentFormat() {
        return notifyContentFormat;
    }

    public void setNotifyContentFormat(String notifyContentFormat) {
        this.notifyContentFormat = notifyContentFormat;
    }
}
