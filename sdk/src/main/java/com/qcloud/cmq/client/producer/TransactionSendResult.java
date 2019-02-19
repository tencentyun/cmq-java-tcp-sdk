package com.qcloud.cmq.client.producer;

import com.qcloud.cmq.client.common.TransactionStatus;

public class TransactionSendResult extends SendResult {

    private TransactionStatus transactionStatus;

    public TransactionSendResult(int returnCode, long msgId, long requestId, String errorMsg, TransactionStatus transactionStatus) {
        super(returnCode, msgId, requestId, errorMsg);
        this.transactionStatus = transactionStatus;
    }

    public TransactionStatus getTransactionStatus() {
        return transactionStatus;
    }
}
