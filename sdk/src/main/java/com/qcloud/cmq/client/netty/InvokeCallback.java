package com.qcloud.cmq.client.netty;

public interface InvokeCallback {
    void operationComplete(final ResponseFuture responseFuture);
}
