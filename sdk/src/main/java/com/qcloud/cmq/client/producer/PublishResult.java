package com.qcloud.cmq.client.producer;

public class PublishResult {

    private int returnCode;
    private long msgId;
    private long requestId;
    private String errorMsg;

    public PublishResult(int returnCode, long msgId, long requestId, String errorMsg) {
        this.returnCode = returnCode;
        this.msgId = msgId;
        this.requestId = requestId;
        this.errorMsg = errorMsg;
    }

    public String toString() {
        return "[returnCode=" + returnCode + ", msgId=" + msgId + ", requestId=" + requestId+ ", errorMsg=" + errorMsg +"]";
    }
}
