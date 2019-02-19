package com.qcloud.cmq.client.consumer;

import java.util.List;

public class BatchDeleteResult {
    private int returnCode;
    private long requestId;
    private String errorMsg;
    private List<ReceiptHandleErrorInfo> errorList;

    public BatchDeleteResult(int returnCode, long requestId, String errorMsg, List<ReceiptHandleErrorInfo> errorList) {
        this.returnCode = returnCode;
        this.requestId = requestId;
        this.errorMsg = errorMsg;
        this.errorList = errorList;
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

    public List<ReceiptHandleErrorInfo> getErrorList() {
        return errorList;
    }
}
