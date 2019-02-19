package com.qcloud.cmq.client.netty;

public class RemoteSendRequestException extends RemoteException {

    private static final long serialVersionUID = 7812906500582486557L;

    public RemoteSendRequestException(String address, Throwable cause) {
        super("send request to <" + address + "> failed", cause);
    }
}
