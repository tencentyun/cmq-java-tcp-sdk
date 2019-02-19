package com.qcloud.cmq.client.consumer;

public interface DeleteCallback {

    void onSuccess(final DeleteResult deleteResult);

    void onException(final Throwable e);
}
