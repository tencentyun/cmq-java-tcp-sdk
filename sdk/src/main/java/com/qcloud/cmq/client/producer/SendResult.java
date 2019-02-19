package com.qcloud.cmq.client.producer;

public class SendResult {

    private int returnCode;
    private long msgId;
    private long requestId;
    private String errorMsg;

    public SendResult(int returnCode, long msgId, long requestId, String errorMsg) {
        this.returnCode = returnCode;
        this.msgId = msgId;
        this.requestId = requestId;
        this.errorMsg = errorMsg;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public long getMsgId() {
        return msgId;
    }

    public long getRequestId() {
        return requestId;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public String toString() {
        return "[returnCode=" + returnCode + ", msgId=" + msgId + ", requestId=" + requestId+ ", errorMsg=" + errorMsg +"]";
    }
}
