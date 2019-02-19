package com.qcloud.cmq.client.consumer;

public interface ReceiveCallback {

    void onSuccess(final ReceiveResult receiveResult);

    void onException(final Throwable e);
}
