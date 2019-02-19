package com.qcloud.cmq.client.producer;

public interface BatchPublishCallback {

    void onSuccess(final BatchPublishResult batchPublishResult);

    void onException(final Throwable e);
}
