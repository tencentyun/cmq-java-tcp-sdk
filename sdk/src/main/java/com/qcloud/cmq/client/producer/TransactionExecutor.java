package com.qcloud.cmq.client.producer;

import com.qcloud.cmq.client.common.TransactionStatus;

/**
 * 本地事务接口
 */
public interface TransactionExecutor {

    /**
     * 应用重写该方法执行本地事务
     * @param msg 消息
     * @param arg 事务参数
     * @return {@link TransactionStatus} 返回事务执行结果
     */
    TransactionStatus execute(String msg, Object arg);
}
