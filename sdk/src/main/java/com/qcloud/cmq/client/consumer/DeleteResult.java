package com.qcloud.cmq.client.consumer;

public class DeleteResult {
    private int returnCode;
    private long requestId;
    private String errorMsg;

    public DeleteResult(int returnCode, long requestId, String errorMsg) {
        this.returnCode = returnCode;
        this.requestId = requestId;
        this.errorMsg = errorMsg;
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
}