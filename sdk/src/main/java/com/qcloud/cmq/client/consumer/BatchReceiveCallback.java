package com.qcloud.cmq.client.consumer;

public interface BatchReceiveCallback {

    void onSuccess(final BatchReceiveResult receiveResult);

    void onException(final Throwable e);
}
