package com.qcloud.cmq.client.producer;

public interface SendCallback {
    void onSuccess(final SendResult sendResult);

    void onException(final Throwable e);
}
