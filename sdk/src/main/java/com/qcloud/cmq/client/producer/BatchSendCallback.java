package com.qcloud.cmq.client.producer;

public interface BatchSendCallback {

    void onSuccess(final BatchSendResult batchSendResult);

    void onException(final Throwable e);
}


