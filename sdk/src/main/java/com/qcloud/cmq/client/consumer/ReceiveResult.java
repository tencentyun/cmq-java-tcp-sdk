package com.qcloud.cmq.client.consumer;

public class ReceiveResult {
    private int returnCode;
    private long requestId;
    private String errorMsg;
    private Message message;

    public ReceiveResult(int returnCode, long requestId, String errorMsg, Message message) {
        this.returnCode = returnCode;
        this.requestId = requestId;
        this.errorMsg = errorMsg;
        this.message = message;
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

    public Message getMessage() {
        return message;
    }
}
