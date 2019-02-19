package com.qcloud.cmq.client.consumer;

import java.util.concurrent.ConcurrentHashMap;

public class Message {
    private long msgId;
    private long receiptHandle;
    private String data;

    public Message(long msgId, long receiptHandle, String data) {
        this.msgId = msgId;
        this.receiptHandle = receiptHandle;
        this.data = data;
    }

    public long getMessageId() {
        return msgId;
    }

    public long getReceiptHandle() {
        return receiptHandle;
    }

    public String getData() {
        return data;
    }

    public String toString() {
        return "[msgId=" + msgId + ", receiptHandle=" + receiptHandle+ ", data=" + data +"]";
    }
}