package com.qcloud.cmq.client.netty;

import com.qcloud.cmq.client.protocol.Cmq;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ResponseFuture {
    private final long timeoutMillis;
    private final InvokeCallback invokeCallback;
    private final long beginTimestamp = System.currentTimeMillis();
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    private final SemaphoreReleaseOnlyOnce once;

    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);
    private volatile Cmq.CMQProto responseCommand;
    private volatile boolean sendRequestOK = true;
    private volatile Throwable cause;

    public ResponseFuture(long timeoutMillis, InvokeCallback invokeCallback, SemaphoreReleaseOnlyOnce once) {
        this.timeoutMillis = timeoutMillis;
        this.invokeCallback = invokeCallback;
        this.once = once;
    }

    public void executeInvokeCallback() {
        if (invokeCallback != null) {
            if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
                invokeCallback.operationComplete(this);
            }
        }
    }

    public void release() {
        if (this.once != null) {
            this.once.release();
        }
    }

    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    public Cmq.CMQProto waitResponse(final long timeoutMillis) throws InterruptedException {
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }

    public void putResponse(final Cmq.CMQProto responseCommand) {
        this.responseCommand = responseCommand;
        this.countDownLatch.countDown();
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public boolean isSendRequestOK() {
        return sendRequestOK;
    }

    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public InvokeCallback getInvokeCallback() {
        return invokeCallback;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public Cmq.CMQProto getResponseCommand() {
        return responseCommand;
    }

    public void setResponseCommand(Cmq.CMQProto responseCommand) {
        this.responseCommand = responseCommand;
    }

    public String getErrorMsg(String prefix) {
        if (!this.isSendRequestOK()) {
            return String.format("%s, send request failed", prefix);
        } else if (this.isTimeout()) {
            return String.format("%s, wait response timeout %d ms", prefix, this.getTimeoutMillis() );
        } else {
            return String.format("%s, unknown reason", prefix);
        }
    }

    @Override
    public String toString() {
        return "ResponseFuture [responseCommand=" + responseCommand + ", sendRequestOK=" + sendRequestOK
                + ", cause=" + cause  + ", timeoutMillis=" + timeoutMillis + ", invokeCallback=" + invokeCallback
                + ", beginTimestamp=" + beginTimestamp + ", countDownLatch=" + countDownLatch + "]";
    }
}
