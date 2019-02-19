package com.qcloud.cmq.client.producer;

public interface PublishCallback {
    void onSuccess(final PublishResult publishResult);

    void onException(final Throwable e);
}
