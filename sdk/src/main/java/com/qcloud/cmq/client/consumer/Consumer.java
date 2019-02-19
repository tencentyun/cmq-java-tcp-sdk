package com.qcloud.cmq.client.consumer;

import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;

import java.util.List;

public class Consumer extends ClientConfig {

    private ConsumerImpl consumer = new ConsumerImpl(this);

    public void start() throws MQClientException {
        consumer.start();
    }

    public ReceiveResult receiveMsg(String queue) throws MQClientException, MQServerException {
        return receiveMsg(queue, this.getPollingWaitSeconds());
    }

    public ReceiveResult receiveMsg(String queue, int pollingWaitingSeconds) throws MQClientException, MQServerException {
        return consumer.receiveMsg(queue, pollingWaitingSeconds, this.getRequestTimeoutMS());
    }

    public void receiveMsg(String queue, ReceiveCallback callback) throws MQClientException, MQServerException {
        receiveMsg(queue, this.getPollingWaitSeconds(), callback);
    }

    public void receiveMsg(String queue, int pollingWaitingSeconds, ReceiveCallback callback) throws MQClientException, MQServerException {
        consumer.receiveMsg(queue, pollingWaitingSeconds, this.getRequestTimeoutMS(), callback);
    }

    public BatchReceiveResult batchReceiveMsg(String queue) throws MQClientException, MQServerException {
        return batchReceiveMsg(queue, this.getBatchPullNumber());
    }

    public BatchReceiveResult batchReceiveMsg(String queue, int number) throws MQClientException, MQServerException {
        return batchReceiveMsg(queue, number, getPollingWaitSeconds());
    }

    public BatchReceiveResult batchReceiveMsg(String queue, int number, int pollingWaitSeconds) throws MQClientException, MQServerException {
        return consumer.batchReceive(queue, number, pollingWaitSeconds, this.getRequestTimeoutMS());
    }

    public void batchReceiveMsg(String queue, BatchReceiveCallback callback) throws MQClientException, MQServerException {
        batchReceiveMsg(queue, this.getBatchPullNumber(), callback);
    }

    public void batchReceiveMsg(String queue, int number, BatchReceiveCallback callback) throws MQClientException, MQServerException {
        batchReceiveMsg(queue, number, getPollingWaitSeconds(),callback);
    }

    public void batchReceiveMsg(String queue, int number, int pollingWaitSeconds, BatchReceiveCallback callback) throws MQClientException, MQServerException {
        consumer.batchReceive(queue, number, pollingWaitSeconds, getRequestTimeoutMS(), callback);
    }

    public DeleteResult deleteMsg(String queue, long receiptHandle) throws MQClientException, MQServerException {
        return consumer.deleteMsg(queue, receiptHandle, this.getRequestTimeoutMS());
    }

    public void deleteMsg(String queue, long receiptHandle, DeleteCallback callback) throws MQClientException, MQServerException {
        consumer.deleteMsg(queue, receiptHandle, this.getRequestTimeoutMS(), callback);
    }

    public BatchDeleteResult batchDeleteMsg(String queue, List<Long> receiptHandleList) throws MQClientException, MQServerException {
        return consumer.batchDelete(queue, receiptHandleList, getRequestTimeoutMS());
    }

    public void batchDeleteMsg(String queue, List<Long> receiptHandleList, BatchDeleteCallback callback) throws MQClientException, MQServerException {
        consumer.batchDelete(queue, receiptHandleList, getRequestTimeoutMS(), callback);
    }

    public void subscribe(String queue, MessageListener listener) throws MQClientException, MQServerException {
        this.consumer.subscriber(queue, listener);
    }

    public void unSubscribe(String queue) throws MQClientException {
        this.consumer.unSubscriber(queue);
    }

    public void shutdown() {
        consumer.shutdown();
    }
}
