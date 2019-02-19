package com.qcloud.cmq.client.producer;

import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.common.TransactionStatus;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;

import java.util.List;

public class Producer extends ClientConfig {

    protected final ProducerImpl producer = new ProducerImpl(this);

    public void start() throws MQClientException {
        producer.start();
    }

    public SendResult send(String queue, String msgBody) throws MQClientException, MQServerException {
        return send(queue, msgBody, -1);
    }

    protected SendResult sendTransactionMsg(String queue, String msgBody, int firstQueryInterval) throws MQClientException, MQServerException {
        return producer.sendTransactionMsg(queue, msgBody, -1, this.getRequestTimeoutMS(), firstQueryInterval);
    }

    public SendResult send(String queue, String msgBody, int delaySeconds) throws MQClientException, MQServerException {
        return producer.send(queue, msgBody, delaySeconds, this.getRequestTimeoutMS());
    }

    public BatchSendResult batchSend(String queue, List<String> msgList) throws MQClientException, MQServerException {
        return batchSend(queue, msgList, -1);
    }

    public BatchSendResult batchSend(String queue, List<String> msgList, int delaySeconds) throws MQClientException, MQServerException {
        return producer.batchSend(queue, msgList, delaySeconds, this.getRequestTimeoutMS());
    }

    protected BatchSendResult batchSendTransactionMsg(String queue, List<String> msgList, int firstQueryInterval) throws MQClientException, MQServerException{
        return producer.batchSendTransactionMsg(queue, msgList, -1, this.getRequestTimeoutMS(), firstQueryInterval);
    }

    public void send(String queue, String msgBody, SendCallback callback) throws MQClientException, MQServerException {
        send(queue, msgBody, -1, callback);
    }

    public void send(String queue, String msgBody, int delaySeconds, SendCallback callback) throws MQClientException, MQServerException {
        producer.send(queue, msgBody, delaySeconds, callback, this.getRequestTimeoutMS());
    }

    public void batchSend(String queue, List<String> msgList, BatchSendCallback callback) throws MQClientException, MQServerException {
        batchSend(queue, msgList, -1, callback);
    }

    public void batchSend(String queue, List<String> msgList, int delaySeconds, BatchSendCallback callback) throws MQClientException, MQServerException {
        producer.batchSend(queue, msgList, delaySeconds, callback, this.getRequestTimeoutMS());
    }


    public PublishResult publish(String topic, String msgBody, String routeKey) throws MQClientException, MQServerException {
        return producer.publish(topic, msgBody, routeKey, this.getRequestTimeoutMS());
    }

    public PublishResult publish(String topic, String msgBody, List<String> tagList) throws MQClientException, MQServerException {
        return producer.publish(topic, msgBody, tagList, this.getRequestTimeoutMS());
    }

    public BatchPublishResult batchPublish(String topic, List<String> msgList, String routeKey) throws MQClientException, MQServerException {
        return producer.batchPublish(topic, msgList, routeKey, this.getRequestTimeoutMS());
    }

    public BatchPublishResult batchPublish(String topic, List<String> msgList, List<String> tagList) throws MQClientException, MQServerException {
        return producer.batchPublish(topic, msgList, tagList, this.getRequestTimeoutMS());
    }

    public void publish(String topic, String msgBody, String routeKey, PublishCallback callback) throws MQClientException, MQServerException {
        producer.publish(topic, msgBody, routeKey, callback, this.getRequestTimeoutMS());
    }

    public void publish(String topic, String msgBody, List<String> tagList, PublishCallback callback) throws MQClientException, MQServerException {
        producer.publish(topic, msgBody, tagList, callback, this.getRequestTimeoutMS());
    }

    public void batchPublish(String topic, List<String> msgList, String routeKey, BatchPublishCallback callback) throws MQClientException, MQServerException {
        producer.batchPublish(topic, msgList, routeKey, callback, this.getRequestTimeoutMS());
    }

    public void batchPublish(String topic, List<String> msgList, List<String> tagList, BatchPublishCallback callback) throws MQClientException, MQServerException {
        producer.batchPublish(topic, msgList, tagList, callback, this.getRequestTimeoutMS());
    }

    public void shutdown() {
        producer.shutdown();
    }

}
