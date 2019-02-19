package com.qcloud.cmq.client.consumer;

import java.util.List;

public class BatchReceiveResult {
    private int returnCode;
    private long requestId;
    private String errorMsg;
    private List<Message> messageList;

    public BatchReceiveResult(int returnCode, long requestId, String errorMsg, List<Message> messageList) {
        this.returnCode = returnCode;
        this.requestId = requestId;
        this.errorMsg = errorMsg;
        this.messageList = messageList;
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

    public List<Message> getMessageList() {
        return messageList;
    }
}
