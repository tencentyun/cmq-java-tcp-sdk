package com.qcloud.cmq.client.consumer;

public class ReceiptHandleErrorInfo {
    private int returnCode;
    private String errorMsg;
    private long receiptHandle;

    public ReceiptHandleErrorInfo(int returnCode, String errorMsg, long receiptHandle) {
        this.returnCode = returnCode;
        this.errorMsg = errorMsg;
        this.receiptHandle = receiptHandle;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public long getReceiptHandle() {
        return receiptHandle;
    }

    public String getErrorMessage() {
        return errorMsg;
    }
}