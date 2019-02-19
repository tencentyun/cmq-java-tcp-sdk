package com.qcloud.cmq.client.producer;

import org.slf4j.Logger;
import com.qcloud.cmq.client.common.*;
import com.qcloud.cmq.client.exception.MQServerException;
import com.qcloud.cmq.client.exception.MQClientException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionProducer extends Producer {

    private final static Logger logger = LogHelper.getLog();
    private int firstCheckInterval = 5000;

    private static ConcurrentHashMap<String/*queue name*/, TransactionStatusChecker> queueChecker
            = new ConcurrentHashMap<String, TransactionStatusChecker>();

    public TransactionProducer() {
    }

    public static TransactionStatusChecker getChecker(String queueName) {
        return TransactionProducer.queueChecker.get(queueName);
    }

    public static void setChecker(String queueName, TransactionStatusChecker checker) {
        TransactionProducer.queueChecker.put(queueName, checker);
    }

    public int getFirstCheckInterval() {
        return firstCheckInterval;
    }

    public void setFirstCheckInterval(int firstCheckInterval) {
        this.firstCheckInterval = firstCheckInterval;
    }

    /**
     * 同步发送单个事务消息
     * @param queue 事务队列名
     * @param message 消息内容
     * @param executor 本地事务执行器
     * @param arg 事务执行器参数
     * @return 本次事务消息的发送结果
     * @throws MQClientException
     */
    public TransactionSendResult sendTransactionMessage(String queue, String message,
                                                        TransactionExecutor executor, Object arg) throws MQClientException {

        if (null == queue || queue == ""){
            throw new MQClientException("Queue name is empty", null);
        }

        if (null == TransactionProducer.queueChecker.get(queue)){
            throw new MQClientException("TransactionStatusChecker is no set", null);
        }

        if (null == executor){
            throw new MQClientException("TransactionExecutor is no set", null);
        }

        SendResult result = null;
        try {
            result = this.sendTransactionMsg(queue, message, this.getFirstCheckInterval());
        } catch (Exception e) {
            throw new MQClientException("Send message error", null);
        }

        if (result.getReturnCode() != ResponseCode.SUCCESS){
            return new TransactionSendResult(result.getReturnCode(),
                                             result.getMsgId(),
                                             result.getRequestId(),
                                             result.getErrorMsg(),
                                             null);
        }

        TransactionStatus transactionStatus = executor.execute(message, arg);
        if (null == transactionStatus){
            transactionStatus = TransactionStatus.UN_KNOW;
        }

        if (transactionStatus != TransactionStatus.SUCCESS){
            logger.info("local executor return [{}]", transactionStatus);
            logger.info(message);
        }

        try {
            sendSecondaryConfirmation(transactionStatus, queue, result);
        } catch (MQServerException e) {
            logger.warn("transaction executor execute " + transactionStatus +  ", send confirm failed!", e);
        }

        return new TransactionSendResult(result.getReturnCode(),
                                         result.getMsgId(),
                                         result.getRequestId(),
                                         result.getErrorMsg(),
                                         transactionStatus);
    }

    /**
     * 同步发送
     * @param queue 事务队列名
     * @param msgList 消息内容列表
     * @param executorList 本地事务执行器列表
     * @param argList 本地事务执行器的参数列表
     * @return 本次批量发送事务消息的结果
     * @throws MQClientException
     */
    public TransactionBatchSendResult batchSendTransactionMessages(String queue, List<String> msgList,
                                                             List<TransactionExecutor> executorList,
                                                             List<Object> argList) throws MQClientException {
        if (null == queue || queue == ""){
            throw new MQClientException("Queue name is empty", null);
        }

        if (null == TransactionProducer.queueChecker.get(queue)){
            throw new MQClientException("TransactionStatusChecker is no set", null);
        }

        if(null == executorList || executorList.contains(null)){
            throw new MQClientException("No executorList is set, or executorList contains null", null);
        }

        if (msgList.size() != executorList.size()){
            throw new MQClientException("The number of message and executor does not match", null);
        }

        BatchSendResult sendResult = null;
        try {
            sendResult = this.batchSendTransactionMsg(queue, msgList, this.getFirstCheckInterval());
        } catch (MQServerException e) {
            throw new MQClientException("Batch send message error", e);
        }

        if (sendResult.getReturnCode() != ResponseCode.SUCCESS){
            return new TransactionBatchSendResult(sendResult.getReturnCode(),
                                                  sendResult.getRequestId(),
                                                  sendResult.getErrorMessage(), null, null);
        }


        List<TransactionStatus> transactionStatusList = new ArrayList<TransactionStatus>();
        for (int i = 0;i < msgList.size(); i++){
            TransactionStatus status = executorList.get(i).execute(msgList.get(i), argList.get(i));
            if (null == status){
                status = TransactionStatus.UN_KNOW;
            }
            if (status != TransactionStatus.SUCCESS){
                logger.info("local transaction return [{}]", status);
            }
            transactionStatusList.add(status);
        }

        try {
            sendSecondaryConfirmation(transactionStatusList, queue, sendResult);
        } catch (MQServerException e) {
            logger.warn("transaction executor execute " + transactionStatusList +  ", send confirm failed {}", e);
        }

        return new TransactionBatchSendResult(
                                            sendResult.getReturnCode(),
                                            sendResult.getRequestId(),
                                            sendResult.getErrorMessage(),
                                            sendResult.getMsgIdList(),
                                            transactionStatusList);
    }

    private void sendSecondaryConfirmation(TransactionStatus transactionStatus,
                                           String queueName,
                                           SendResult sendResult) throws MQClientException, MQServerException {
        if (null == transactionStatus){
            throw new MQClientException("transactionStatus is empty", null);
        }

        if (null == queueName || queueName == ""){
            throw new MQClientException("Queue name is empty", null);
        }

        if (null == sendResult){
            throw new MQClientException("sendResult is empty", null);
        }

        this.producer.sendConfirmMsgImpl(transactionStatus, queueName, sendResult, this.getRequestTimeoutMS());
    }

    private void sendSecondaryConfirmation(List<TransactionStatus> transactionStatus,
                                          String queueName,
                                          BatchSendResult sendResult) throws MQClientException, MQServerException {
        this.producer.sendConfirmMsgImpl(transactionStatus, queueName, sendResult, this.getRequestTimeoutMS());
    }
}
