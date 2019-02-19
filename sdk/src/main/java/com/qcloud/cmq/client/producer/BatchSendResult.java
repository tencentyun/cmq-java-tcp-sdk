package com.qcloud.cmq.client.producer;

import java.util.List;

public class BatchSendResult {
    private int returnCode;
    private long requestId;
    private String errorMsg;
    private List<Long> msgIdList;

    public BatchSendResult(int returnCode, long requestId, String errorMsg, List<Long> msgIdList) {
        this.returnCode = returnCode;
        this.requestId = requestId;
        this.errorMsg = errorMsg;
        this.msgIdList = msgIdList;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public long getRequestId() {
        return requestId;
    }

    public String getErrorMessage() {
        return errorMsg;
    }

    public List<Long> getMsgIdList() {
        return msgIdList;
    }

    public String toString() {
        return "[returnCode=" + returnCode + ", requestId=" + requestId+ ", errorMsg=" + errorMsg  + ", msgIdList=" + msgIdList +"]";
    }
}