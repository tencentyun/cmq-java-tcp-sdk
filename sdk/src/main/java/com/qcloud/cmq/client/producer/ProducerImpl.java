package com.qcloud.cmq.client.producer;

import com.google.protobuf.ByteString;
import com.qcloud.cmq.client.common.*;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;
import com.qcloud.cmq.client.client.MQClientInstance;
import com.qcloud.cmq.client.client.MQClientManager;
import com.qcloud.cmq.client.netty.CommunicationMode;
import com.qcloud.cmq.client.netty.RemoteException;
import com.qcloud.cmq.client.protocol.Cmq;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ProducerImpl {
    private static final Logger logger = LogHelper.getLog();
    private final Producer producer;
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mQClientInstance;

    private ConcurrentHashMap<String, List<String>> topicRouteTable = new ConcurrentHashMap<String, List<String>>();
    private ConcurrentHashMap<String, List<String>> queueRouteTable = new ConcurrentHashMap<String, List<String>>();

    ProducerImpl(final Producer producer) {
        this.producer = producer;
    }

    void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;
                this.producer.changeInstanceNameToPID();
                this.mQClientInstance = MQClientManager.getInstance().getAndCreateMQClientInstance(this.producer);
                mQClientInstance.registerProducer(this);
                mQClientInstance.start();
                logger.info("the producer [{}] start OK.", this.producer);
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The producer service state not OK, maybe started once, " + this.serviceState, null);
            default:
                break;
        }
    }

    void shutdown() {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.mQClientInstance.unRegisterProducer(this);
                this.mQClientInstance.shutdown();
                logger.info("the producer [{}] shutdown OK", this.producer);
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException(ResponseCode.SERVICE_STATE_ERROR, "The producer service state not OK, state:" + this.serviceState);
        }
    }

    public SendResult send(String queue, String msgBody, int delaySeconds, long timeout)
            throws MQClientException, MQServerException {
        return this.sendImpl(queue, msgBody, delaySeconds, CommunicationMode.SYNC, null, timeout);
    }

    void send(String queue, String msgBody, int delaySeconds, SendCallback sendCallback, long timeout)
            throws MQClientException, MQServerException {
        this.sendImpl(queue, msgBody, delaySeconds, CommunicationMode.ASYNC, sendCallback, timeout);
    }

    public SendResult sendTransactionMsg(String queue, String msgBody, int delaySeconds, long timeout, int firstQuery)
            throws MQClientException, MQServerException {
        return this.sendImpl(queue, msgBody, delaySeconds, CommunicationMode.SYNC, null, timeout, true, firstQuery);
    }

    private SendResult sendImpl(String queue, String msgBody, int delaySeconds, CommunicationMode communicationMode,
                                SendCallback sendCallback, long timeout) throws MQClientException, MQServerException{
        return sendImpl(queue, msgBody, delaySeconds, communicationMode, sendCallback, timeout, false, 0);
    }

    private SendResult sendImpl(String queue, String msgBody, int delaySeconds, CommunicationMode communicationMode,
                                SendCallback sendCallback, long timeout, boolean ifTrans, int firstQuery)
                                throws MQClientException, MQServerException {
        this.makeSureStateOK();
        Cmq.cmq_tcp_send_msg.Builder contentBuilder = Cmq.cmq_tcp_send_msg.newBuilder()
                .setQueueName(queue)
                .setMsgBody(ByteString.copyFrom(msgBody.getBytes()));
        if (delaySeconds >= 0) {
            contentBuilder.setDelaySeconds(delaySeconds);
        }
        if (ifTrans){
            contentBuilder.setIsTransaction(1);
            contentBuilder.setFirstQueryInterval(firstQuery);
        }
        Cmq.CMQProto request = Cmq.CMQProto.newBuilder()
                .setCmd(Cmq.CMQ_CMD.CMQ_TCP_SEND_MSG_VALUE)
                .setSeqno(RequestIdHelper.getNextSeqNo())
                .setRequestId(RequestIdHelper.getRequestId())
                .setTcpSendMsg(contentBuilder)
                .build();

        int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + this.producer.getRetryTimesWhenSendFailed() : 1;
        for (int times = 0; times < timesTotal; times++) {
            List<String> accessList = findQueueRoute(queue, times == 1);
            try {
                return this.mQClientInstance.getCMQClient().sendMessage(accessList, request, timeout,
                        communicationMode, sendCallback, producer.getRetryTimesWhenSendFailed(), this);
            } catch (RemoteException e) {
                logger.error("send msg error", e);
            } catch (InterruptedException e) {
                logger.error("send msg error", e);
            }
        }
        throw new MQServerException(ResponseCode.SEND_REQUEST_ERROR, String.format("Send Message Error %d times", timesTotal));
    }

    BatchSendResult batchSend(String queue, List<String> msgList, int delaySeconds, long timeout)
            throws MQClientException, MQServerException {
        return batchSendImpl(queue, msgList, delaySeconds, CommunicationMode.SYNC, null, timeout, false, 0);
    }

    void batchSend(String queue, List<String> msgList, int delaySeconds, BatchSendCallback sendCallback, long timeout)
            throws MQClientException, MQServerException {
        this.batchSendImpl(queue, msgList, delaySeconds, CommunicationMode.ASYNC, sendCallback, timeout, false, 0);
    }

    BatchSendResult batchSendTransactionMsg(String queue, List<String> msgList, int delaySeconds, long timeout, int firstQuery)
            throws MQClientException, MQServerException{
        return this.batchSendImpl(queue, msgList, delaySeconds, CommunicationMode.SYNC, null, timeout, true, firstQuery);
    }

    private BatchSendResult batchSendImpl(String queue, List<String> msgList, int delaySeconds, CommunicationMode communicationMode,
                                          final BatchSendCallback sendCallback, final long timeout, boolean ifTrans, int firstQuery)
            throws MQClientException, MQServerException {
        this.makeSureStateOK();

        Cmq.cmq_tcp_batch_send_msg.Builder contentBuilder = Cmq.cmq_tcp_batch_send_msg.newBuilder()
                .setQueueName(queue);
        if (delaySeconds >= 0) {
            contentBuilder.setDelaySeconds(delaySeconds);
        }
        for (String msg: msgList) {
            contentBuilder.addMsgBody(ByteString.copyFrom(msg.getBytes()));
        }

        if (ifTrans){
            contentBuilder.setIsTransaction(1);
            contentBuilder.setFirstQueryInterval(firstQuery);
        }

        Cmq.CMQProto request = Cmq.CMQProto.newBuilder()
                .setCmd(Cmq.CMQ_CMD.CMQ_TCP_BATCH_SEND_MSG_VALUE)
                .setSeqno(RequestIdHelper.getNextSeqNo())
                .setRequestId(RequestIdHelper.getRequestId())
                .setTcpBatchSendMsg(contentBuilder)
                .build();
        int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + this.producer.getRetryTimesWhenSendFailed() : 1;
        for (int times = 0; times < timesTotal; times++) {
            List<String> accessList = this.findQueueRoute(queue, times == 1);
            try {
                return this.mQClientInstance.getCMQClient().batchSendMessage(accessList, request, timeout,
                        communicationMode, sendCallback, producer.getRetryTimesWhenSendFailed(), this);
            } catch (RemoteException e) {
                logger.error("send msg error", e);
            } catch (InterruptedException e) {
                logger.error("send msg error", e);
            }
        }
        throw new MQServerException(ResponseCode.SEND_REQUEST_ERROR, String.format("Send Message Error %d times", timesTotal));
    }

    PublishResult publish(String topic, String msgBody, String routeKey, long timeout) throws MQClientException, MQServerException {
        return publishImpl(topic, msgBody, routeKey, null, CommunicationMode.SYNC, null, timeout);
    }

    void publish(String topic, String msgBody, String routeKey, PublishCallback callback, long timeout) throws MQClientException, MQServerException {
        publishImpl(topic, msgBody, routeKey, null, CommunicationMode.ASYNC, callback, timeout);
    }

    PublishResult publish(String topic, String msgBody, List<String> tagList, long timeout) throws MQClientException, MQServerException {
        return publishImpl(topic, msgBody, null, tagList, CommunicationMode.SYNC, null, timeout);
    }

    public void publish(String topic, String msgBody, List<String> tagList, PublishCallback callback, long timeout) throws MQClientException, MQServerException {
        publishImpl(topic, msgBody, null, tagList, CommunicationMode.ASYNC, callback, timeout);
    }

    private PublishResult publishImpl(String topic, String msgBody, String routeKey, List<String> tagList,
                                   CommunicationMode communicationMode, PublishCallback callback, long timeout) throws MQClientException, MQServerException {
        this.makeSureStateOK();

        Cmq.cmq_tcp_publish_msg.Builder contentBuilder = Cmq.cmq_tcp_publish_msg.newBuilder()
                .setTopicName(topic).setMsgBody(ByteString.copyFrom(msgBody.getBytes()));
        if (routeKey != null) {
            contentBuilder.setRoutingKey(routeKey);
        }
        if (tagList != null && !tagList.isEmpty()) {
            contentBuilder.addAllMsgTags(tagList);
        }
        Cmq.CMQProto request = Cmq.CMQProto.newBuilder()
                .setCmd(Cmq.CMQ_CMD.CMQ_TCP_PUBLISH_MSG_VALUE)
                .setSeqno(RequestIdHelper.getNextSeqNo())
                .setRequestId(RequestIdHelper.getRequestId())
                .setTcpPublishMsg(contentBuilder)
                .build();
        int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + this.producer.getRetryTimesWhenSendFailed() : 1;
        for (int times = 0; times < timesTotal; times++) {
            List<String> accessList = this.findTopicRoute(topic, times==1);
            try {
                return this.mQClientInstance.getCMQClient().publishMessage(accessList, request, timeout,
                        communicationMode, callback, producer.getRetryTimesWhenSendFailed(), this);
            } catch (RemoteException e) {
                logger.error("send msg error", e);
            } catch (InterruptedException e) {
                logger.error("send msg error", e);
            }
        }
        throw new MQServerException(ResponseCode.SEND_REQUEST_ERROR, String.format("Send Message Error %d times", timesTotal));
    }

    BatchPublishResult batchPublish(String topic, List<String> msgList, String routeKey, long timeout) throws MQClientException, MQServerException {
        return batchPublishImpl(topic, msgList, routeKey, null, CommunicationMode.SYNC, null, timeout);
    }

    void batchPublish(String topic, List<String> msgList, String routeKey, BatchPublishCallback callback, long timeout) throws MQClientException, MQServerException {
        batchPublishImpl(topic, msgList, routeKey, null, CommunicationMode.ASYNC, callback, timeout);
    }

    BatchPublishResult batchPublish(String topic, List<String> msgList, List<String> tagList, long timeout) throws MQClientException, MQServerException {
        return batchPublishImpl(topic, msgList, null, tagList, CommunicationMode.SYNC, null, timeout);
    }

    void batchPublish(String topic, List<String> msgList, List<String> tagList, BatchPublishCallback callback, long timeout) throws MQClientException, MQServerException {
        batchPublishImpl(topic, msgList, null, tagList, CommunicationMode.ASYNC, callback, timeout);
    }

    private BatchPublishResult batchPublishImpl(String topic, List<String> msgList, String routeKey, List<String> tagList,
                                             CommunicationMode communicationMode, BatchPublishCallback callback, long timeout)
            throws MQClientException, MQServerException {
        this.makeSureStateOK();

        Cmq.cmq_tcp_batch_publish_msg.Builder contentBuilder = Cmq.cmq_tcp_batch_publish_msg.newBuilder()
                .setTopicName(topic);
        if (routeKey != null) {
            contentBuilder.setRoutingKey(routeKey);
        }
        if (tagList != null && !tagList.isEmpty()) {
            contentBuilder.addAllMsgTags(tagList);
        }
        for (String msg: msgList) {
            contentBuilder.addMsgBody(ByteString.copyFrom(msg.getBytes()));
        }
        Cmq.CMQProto request = Cmq.CMQProto.newBuilder()
                .setCmd(Cmq.CMQ_CMD.CMQ_TCP_BATCH_PUBLISH_MSG_VALUE)
                .setSeqno(RequestIdHelper.getNextSeqNo())
                .setRequestId(RequestIdHelper.getRequestId())
                .setTcpBatchPublishMsg(contentBuilder)
                .build();
        int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + this.producer.getRetryTimesWhenSendFailed() : 1;
        for (int times = 0; times < timesTotal; times++) {
            List<String> accessList = this.findTopicRoute(topic, times==1);
            try {
                return this.mQClientInstance.getCMQClient().batchPublishMessage(accessList, request, timeout,
                        communicationMode, callback, producer.getRetryTimesWhenSendFailed(), this);
            } catch (RemoteException e) {
                logger.error("send msg error", e);
            } catch (InterruptedException e) {
                logger.error("send msg error", e);
            }
        }
        throw new MQServerException(ResponseCode.SEND_REQUEST_ERROR, String.format("Send Message Error %d times", timesTotal));
    }

    public List<String> findTopicRoute(String topic, boolean needUpdate) throws MQServerException, MQClientException {
        List<String> accessList = this.topicRouteTable.get(topic);
        if (accessList == null || accessList.isEmpty() || needUpdate) {
            this.mQClientInstance.updateTopicRoute(topic, topicRouteTable);
            accessList = this.topicRouteTable.get(topic);
            if (accessList == null || accessList.isEmpty()) {
                throw new MQServerException(ResponseCode.ROUTE_NOT_FOUND, String.format("route for topic[%s] not found", topic));
            }
        }
        return accessList;
    }

    public List<String> findQueueRoute(String queue, boolean needUpdate) throws MQServerException, MQClientException {
        List<String> accessList = this.queueRouteTable.get(queue);
        if (accessList == null || accessList.isEmpty() || needUpdate) {
            this.mQClientInstance.updateQueueRoute(queue, queueRouteTable);
            accessList = this.queueRouteTable.get(queue);
            if (accessList == null || accessList.isEmpty()) {
                throw new MQServerException(ResponseCode.ROUTE_NOT_FOUND, String.format("route for queue[%s] not found", queue));
            }
        }
        return accessList;
    }

    public void sendConfirmMsgImpl(List<TransactionStatus> transactionStatusList,
                                   String queueName,
                                   BatchSendResult sendResult, long timeout) throws MQServerException, MQClientException {

        Cmq.cmq_transaction_confirm_item.Builder itemBuilder = Cmq.cmq_transaction_confirm_item.newBuilder();
        Cmq.cmq_transaction_confirm.Builder builder = Cmq.cmq_transaction_confirm.newBuilder();
        builder.setQueueName(queueName);

        for (int i = 0;i < transactionStatusList.size(); i++){
            long transactionId = sendResult.getMsgIdList().get(i);
            itemBuilder.setMsgId(transactionId);

            switch (transactionStatusList.get(i)){
                case SUCCESS:
                    itemBuilder.setState(ConfirmState.COMMIT);
                    break;
                case FAIL:
                    itemBuilder.setState(ConfirmState.ROLLBACK);
                    break;
                case UN_KNOW:
                    continue;
            }
            builder.addItem(itemBuilder);
        }

        if (builder.getItemCount() == 0){
            logger.info("no confirm!");
            return;
        }
        Cmq.CMQProto confirmRequest = Cmq.CMQProto.newBuilder()
                .setCmd(Cmq.CMQ_CMD.CMQ_TRANSACTION_CONFIRM_VALUE)
                .setSeqno(RequestIdHelper.getNextSeqNo())
                .setTransactionConfirm(builder).build();

        List<String> accessList = findQueueRoute(queueName, true);
        try {
            this.mQClientInstance.getCMQClient().sendMessage(accessList, confirmRequest, timeout,
                    CommunicationMode.ONEWAY, null, producer.getRetryTimesWhenSendFailed(), this);
        } catch (RemoteException e) {
            logger.error("send msg error:{}", e);
        } catch (InterruptedException e) {
            logger.error("send msg error:{}", e);
        }

        logger.info("send confirm!");
    }

    public void sendConfirmMsgImpl(TransactionStatus transactionStatus,
                               String queueName,
                               SendResult sendResult, long timeout) throws MQServerException, MQClientException {
        long transactionId = sendResult.getMsgId();
        Cmq.cmq_transaction_confirm_item.Builder itemBuilder = Cmq.cmq_transaction_confirm_item.newBuilder()
                .setMsgId(transactionId);

        switch (transactionStatus){
            case SUCCESS:
                itemBuilder.setState(ConfirmState.COMMIT);
                break;
            case FAIL:
                itemBuilder.setState(ConfirmState.ROLLBACK);
                break;
            case UN_KNOW:
                return ;
        }

        Cmq.cmq_transaction_confirm.Builder builder = Cmq.cmq_transaction_confirm.newBuilder().
                setQueueName(queueName).
                addItem(itemBuilder);

        Cmq.CMQProto confirmRequest = Cmq.CMQProto.newBuilder()
                .setCmd(Cmq.CMQ_CMD.CMQ_TRANSACTION_CONFIRM_VALUE)
                .setSeqno(RequestIdHelper.getNextSeqNo())
                .setTransactionConfirm(builder).build();

        List<String> accessList = findQueueRoute(queueName, true);
        try {
            this.mQClientInstance.getCMQClient().sendMessage(accessList, confirmRequest, timeout,
                    CommunicationMode.ONEWAY, null, producer.getRetryTimesWhenSendFailed(), this);
            logger.info("send second confirm message");
        } catch (RemoteException e) {
            logger.error("send msg error:{}", e);
        } catch (InterruptedException e) {
            logger.error("send msg error:{}", e);
        }

    }
}
