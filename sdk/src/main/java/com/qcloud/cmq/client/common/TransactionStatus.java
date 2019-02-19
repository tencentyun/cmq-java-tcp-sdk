package com.qcloud.cmq.client.common;

/**
 * 本地事务操作的状态
 */
public enum TransactionStatus {
    /**
     * 执行成功，消费者会收到对应的事务消息
     */
    SUCCESS,

    /**
     * 执行失败，CMQ会回滚该半消息，消费者不会收到该事务消息
     */
    FAIL,

    /**
     * 状态异常或者事务未运行出结果，Broker会回查结果
     */
    UN_KNOW
}
