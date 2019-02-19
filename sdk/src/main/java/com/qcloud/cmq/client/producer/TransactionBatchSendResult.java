package com.qcloud.cmq.client.producer;

import com.qcloud.cmq.client.common.TransactionStatus;

import java.util.List;

public class TransactionBatchSendResult extends BatchSendResult {

    private List<TransactionStatus> transactionStatusList;

    public TransactionBatchSendResult(int returnCode, long requestId, String errorMsg, List<Long> msgIdList, List<TransactionStatus> transactionStatusList) {
        super(returnCode, requestId, errorMsg, msgIdList);
        this.transactionStatusList = transactionStatusList;
    }

    public List<TransactionStatus> getTransactionStatusList() {
        return transactionStatusList;
    }
}
