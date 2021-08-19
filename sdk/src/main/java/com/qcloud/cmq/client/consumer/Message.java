package com.qcloud.cmq.client.consumer;

public class Message {

    private long msgId;
    private long receiptHandle;
    private String data;
    private long enqueueTime;
    private long nextVisibleTime;
    private long firstDequeueTime;
    private long dequeueCount;

    public Message() {
    }

    public Message(long msgId, long receiptHandle, String data, long enqueueTime, long nextVisibleTime, long firstDequeueTime, long dequeueCount) {
        this.msgId = msgId;
        this.receiptHandle = receiptHandle;
        this.data = data;
        this.enqueueTime = enqueueTime;
        this.nextVisibleTime = nextVisibleTime;
        this.firstDequeueTime = firstDequeueTime;
        this.dequeueCount = dequeueCount;
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

    public long getEnqueueTime() {
        return enqueueTime;
    }

    public long getNextVisibleTime() {
        return nextVisibleTime;
    }

    public long getFirstDequeueTime() {
        return firstDequeueTime;
    }

    public long getDequeueCount() {
        return dequeueCount;
    }

    @Override
    public String toString() {
        return "Message{" +
                "msgId=" + msgId +
                ", receiptHandle=" + receiptHandle +
                ", data='" + data + '\'' +
                ", enqueueTime=" + enqueueTime +
                ", nextVisibleTime=" + nextVisibleTime +
                ", firstDequeueTime=" + firstDequeueTime +
                ", dequeueCount=" + dequeueCount +
                '}';
    }
}
