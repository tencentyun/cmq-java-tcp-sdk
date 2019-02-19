package com.qcloud.cmq.client.consumer;

public interface BatchDeleteCallback {

    void onSuccess(final BatchDeleteResult deleteResult);

    void onException(final Throwable e);
}
