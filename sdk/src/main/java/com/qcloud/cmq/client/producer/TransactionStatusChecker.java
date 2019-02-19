package com.qcloud.cmq.client.producer;

import com.qcloud.cmq.client.common.TransactionStatus;
import com.qcloud.cmq.client.consumer.Message;

/**
 * 检查本地事务操作的结果
 */
public interface TransactionStatusChecker {

    /**
     * Broker回查事务结果时调用的函数，Broker根据该接口的返回值决定提交还是回滚
     *
     * @param msg 消息头部带有事务ID
     * @return
     */
    TransactionStatus checkStatus(Message msg);
}
