package com.qcloud.cmq.client.consumer;

import com.google.protobuf.TextFormat;
import com.qcloud.cmq.client.common.ServiceState;
import com.qcloud.cmq.client.client.MQClientManager;
import com.qcloud.cmq.client.common.LogHelper;
import com.qcloud.cmq.client.common.ResponseCode;
import com.qcloud.cmq.client.common.RequestIdHelper;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.client.MQClientInstance;
import com.qcloud.cmq.client.exception.MQServerException;
import com.qcloud.cmq.client.netty.CommunicationMode;
import com.qcloud.cmq.client.netty.RemoteException;
import com.qcloud.cmq.client.protocol.Cmq;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public class ConsumerImpl {

    private final Logger logger = LogHelper.getLog();
    private  MQClientInstance mQClientInstance;
    private final Consumer consumer;
    private final ConcurrentHashMap<String, List<String>> queueRouteTable = new ConcurrentHashMap<String, List<String>>();
    private final ConcurrentHashMap<String, SubscribeService> subscribeTable = new ConcurrentHashMap<String, SubscribeService>();
    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

    private volatile boolean needUpdateRoute = true;

    ConsumerImpl(Consumer consumer) {
        this.consumer = consumer;
    }

    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, " + this.serviceState, null);
        }
    }

    List<String> getQueueRoute(String queue) throws MQClientException {
        List<String> accessList = this.queueRouteTable.get(queue);
        if (accessList == null || accessList.isEmpty() || needUpdateRoute) {
            this.mQClientInstance.updateQueueRoute(queue, queueRouteTable);
            accessList = this.queueRouteTable.get(queue);
            needUpdateRoute = false;
        }
        return accessList;
    }

    public void setNeedUpdateRoute() {
        needUpdateRoute = true;
    }

    public synchronized void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;
                this.mQClientInstance = MQClientManager.getInstance().getAndCreateMQClientInstance(this.consumer);
                mQClientInstance.registerConsumer(this);
                mQClientInstance.start();
                logger.info("the consumer [{}] start OK", this.consumer);
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException(ResponseCode.SERVICE_STATE_ERROR,
                        "The consumer service state not OK, maybe started once, state:" + this.serviceState);
            default:
                break;
        }

    }

    public synchronized void shutdown() {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.mQClientInstance.unRegisterConsumer(this);
                this.mQClientInstance.shutdown();
                logger.info("the consumer [{}] shutdown OK", this.consumer);
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    ReceiveResult receiveMsg(String queue, int pollingWaitSeconds, long timeoutMillis) throws MQClientException, MQServerException {
        return receiveImpl(queue, pollingWaitSeconds, timeoutMillis, CommunicationMode.SYNC, null);
    }

    void receiveMsg(String queue, int pollingWaitSeconds, long timeoutMillis, ReceiveCallback callback) throws MQClientException, MQServerException {
        receiveImpl(queue, pollingWaitSeconds, timeoutMillis, CommunicationMode.ASYNC, callback);
    }

    private ReceiveResult receiveImpl(String queue, int pollingWaitSeconds, long timeoutMillis, CommunicationMode communicationMode,
                                      ReceiveCallback callback) throws MQClientException, MQServerException {
        this.makeSureStateOK();

        if (pollingWaitSeconds < 0) {
            throw new MQClientException("pollingWaitSeconds < 0", null);
        }

        Cmq.cmq_tcp_pull_msg_req.Builder contentBuilder = Cmq.cmq_tcp_pull_msg_req.newBuilder()
                .setQueueName(queue)
                .setPollWaitSeconds(pollingWaitSeconds);
        Cmq.CMQProto request = Cmq.CMQProto.newBuilder()
                .setCmd(Cmq.CMQ_CMD.CMQ_TCP_PULL_MSG_VALUE)
                .setSeqno(RequestIdHelper.getNextSeqNo())
                .setTcpPullMsg(contentBuilder).build();
        timeoutMillis += pollingWaitSeconds * 1000;
        List<String> accessList = this.getQueueRoute(queue);
        try {
            return this.mQClientInstance.getCMQClient().receiveMessage(accessList, request,
                    timeoutMillis, communicationMode, callback);
        } catch (RemoteException e) {
            this.setNeedUpdateRoute();
            logger.error(String.format("receive msg from queue[%s] error", queue), e);
            throw new MQServerException(ResponseCode.SEND_REQUEST_ERROR, "receive msg with error:" + e.getLocalizedMessage());
        } catch (InterruptedException e) {
            this.setNeedUpdateRoute();
            logger.error(String.format("receive msg from queue[%s] error", queue), e);
            throw new MQServerException(ResponseCode.SEND_REQUEST_ERROR, "receive msg with error:" + e.getLocalizedMessage());
        }
    }

    BatchReceiveResult batchReceive(String queue, int maxNums, int pollingWaitSeconds, long timeoutMillis)
            throws MQClientException, MQServerException {
        return batchReceiveImpl(queue, maxNums, pollingWaitSeconds, timeoutMillis, CommunicationMode.SYNC, null);
    }

    void batchReceive(String queue, int maxNums, int pollingWaitSeconds, long timeoutMillis, BatchReceiveCallback callback)
            throws MQClientException, MQServerException {
        batchReceiveImpl(queue, maxNums, pollingWaitSeconds, timeoutMillis, CommunicationMode.ASYNC, callback);
    }

    private BatchReceiveResult batchReceiveImpl(String queue, int maxNums, int pollingWaitSeconds, long timeoutMillis, CommunicationMode communicationMode,
                                                BatchReceiveCallback callback) throws MQClientException, MQServerException {

        this.makeSureStateOK();

        if (pollingWaitSeconds < 0) {
            throw new MQClientException("pollingWaitSeconds < 0", null);
        }
        if (maxNums < 0) {
            throw new MQClientException("maxNums < 0", null);
        }

        Cmq.cmq_tcp_batch_pull_msg_req.Builder contentBuilder = Cmq.cmq_tcp_batch_pull_msg_req.newBuilder()
                .setQueueName(queue)
                .setPollWaitSeconds(pollingWaitSeconds).setNumMsg(maxNums);
        timeoutMillis += pollingWaitSeconds * 1000;
        Cmq.CMQProto request = Cmq.CMQProto.newBuilder()
                .setCmd(Cmq.CMQ_CMD.CMQ_TCP_BATCH_PULL_MSG_VALUE)
                .setSeqno(RequestIdHelper.getNextSeqNo())
                .setTcpBatchPullMsg(contentBuilder).build();

        List<String> accessList = this.getQueueRoute(queue);
        try {
            return this.mQClientInstance.getCMQClient().batchReceiveMessage(accessList, request,
                    timeoutMillis, communicationMode, callback);
        } catch (RemoteException e) {
            this.setNeedUpdateRoute();
            logger.error(String.format("batch receive msg from queue[%s] error", queue), e);
            throw new MQServerException(ResponseCode.SEND_REQUEST_ERROR, "batch receive msg with error:" + e.getLocalizedMessage());
        } catch (InterruptedException e) {
            this.setNeedUpdateRoute();
            logger.error(String.format("batch receive msg from queue[%s] error", queue), e);
            throw new MQServerException(ResponseCode.SEND_REQUEST_ERROR, "batch receive msg with error:" + e.getLocalizedMessage());
        }
    }

    DeleteResult deleteMsg(String queue, long receiptHandle, long timeoutMillis) throws MQClientException, MQServerException {
        return deleteImpl(queue, receiptHandle, timeoutMillis, CommunicationMode.SYNC, null);
    }

    void deleteMsg(String queue, long receiptHandle, long timeoutMillis, DeleteCallback callback) throws MQClientException, MQServerException {
        deleteImpl(queue, receiptHandle, timeoutMillis, CommunicationMode.ASYNC, callback);
    }

    private DeleteResult deleteImpl(String queue, long receiptHandle, long timeoutMillis, CommunicationMode communicationMode,
                                    DeleteCallback callback) throws MQClientException, MQServerException {
        this.makeSureStateOK();

        Cmq.cmq_tcp_delete_msg.Builder contentBuilder = Cmq.cmq_tcp_delete_msg.newBuilder()
                .setQueueName(queue)
                .setReceiptHandle(receiptHandle);
        Cmq.CMQProto request = Cmq.CMQProto.newBuilder()
                .setCmd(Cmq.CMQ_CMD.CMQ_TCP_DELETE_MSG_VALUE)
                .setSeqno(RequestIdHelper.getNextSeqNo())
                .setTcpDeleteMsg(contentBuilder).build();
        List<String> accessList = this.getQueueRoute(queue);
        try {
            return this.mQClientInstance.getCMQClient().deleteMessage(accessList, request,
                    timeoutMillis, communicationMode, callback);
        } catch (RemoteException e) {
            this.setNeedUpdateRoute();
            logger.error(String.format("delete msg from queue[%s] error", queue), e);
            throw new MQServerException(ResponseCode.SEND_REQUEST_ERROR, "delete msg with error:" + e.getLocalizedMessage());
        } catch (InterruptedException e) {
            this.setNeedUpdateRoute();
            logger.error(String.format("delete msg from queue[%s] error", queue), e);
            throw new MQServerException(ResponseCode.SEND_REQUEST_ERROR, "delete msg with error:" + e.getLocalizedMessage());
        }
    }

    BatchDeleteResult batchDelete(String queue, List<Long> receiptHandleList, long timeoutMillis)
            throws MQClientException, MQServerException {
        return batchDeleteImpl(queue, receiptHandleList, timeoutMillis, CommunicationMode.SYNC, null);
    }

    void batchDelete(String queue, List<Long> receiptHandleList, long timeoutMillis, BatchDeleteCallback callback)
            throws MQClientException, MQServerException {
        batchDeleteImpl(queue, receiptHandleList, timeoutMillis, CommunicationMode.ASYNC, callback);
    }

    private BatchDeleteResult batchDeleteImpl(String queue, List<Long> receiptHandleList, long timeoutMillis, CommunicationMode communicationMode,
                                              BatchDeleteCallback callback) throws MQClientException, MQServerException {
        this.makeSureStateOK();

        Cmq.cmq_tcp_batch_delete_msg.Builder contentBuilder = Cmq.cmq_tcp_batch_delete_msg.newBuilder()
                .setQueueName(queue).addAllReceiptHandleList(receiptHandleList);
        Cmq.CMQProto request = Cmq.CMQProto.newBuilder()
                .setCmd(Cmq.CMQ_CMD.CMQ_TCP_BATCH_DELETE_MSG_VALUE)
                .setSeqno(RequestIdHelper.getNextSeqNo())
                .setTcpBatchDeleteMsg(contentBuilder).build();

        List<String> accessList = this.getQueueRoute(queue);
        try {
            return this.mQClientInstance.getCMQClient().batchDeleteMessage(accessList, request,
                    timeoutMillis, communicationMode, callback);
        } catch (RemoteException e) {
            this.setNeedUpdateRoute();
            logger.error(String.format("batch delete msg from queue[%s] error", queue), e);
            throw new MQServerException(ResponseCode.SEND_REQUEST_ERROR, "batch delete msg with error:" + e.getLocalizedMessage());
        } catch (InterruptedException e) {
            this.setNeedUpdateRoute();
            logger.error(String.format("batch delete msg from queue[%s] error", queue), e);
            throw new MQServerException(ResponseCode.SEND_REQUEST_ERROR, "batch delete msg with error:" + e.getLocalizedMessage());
        }
    }

    void subscriber(String queue, MessageListener listener) throws MQClientException, MQServerException {
        this.makeSureStateOK();

        Cmq.cmq_tcp_batch_pull_msg_req.Builder contentBuilder = Cmq.cmq_tcp_batch_pull_msg_req.newBuilder()
                .setQueueName(queue)
                .setPollWaitSeconds(this.consumer.getPollingWaitSeconds())
                .setNumMsg(this.consumer.getBatchPullNumber());
        Cmq.CMQProto.Builder builder = Cmq.CMQProto.newBuilder()
                .setCmd(Cmq.CMQ_CMD.CMQ_TCP_BATCH_PULL_MSG_VALUE)
                .setSeqno(RequestIdHelper.getNextSeqNo())
                .setTcpBatchPullMsg(contentBuilder);
        SubscribeService pullMessageService = new SubscribeService(queue, listener, builder,this);
        if (subscribeTable.putIfAbsent(queue, pullMessageService) == null) {
            pullMessageService.start();
        } else {
            logger.error("queue[%s] already subscribed.", queue);
            throw new MQClientException(ResponseCode.ALREADY_SUBSCRIBED, "queue[" + queue + "] already subscribed.");
        }
        logger.info("subscribe queue {} success.", queue);
    }

    void unSubscriber(String queue) throws MQClientException {
        this.makeSureStateOK();
        SubscribeService pullMessageService = subscribeTable.remove(queue);
        if (pullMessageService != null) {
            pullMessageService.shutdown();
        }
        logger.info("unSubscribe queue {} success.", queue);
    }

    MQClientInstance getMQClientInstance() {
        return  mQClientInstance;
    }

    Consumer getConsumer() {
        return consumer;
    }
}
