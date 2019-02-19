package com.qcloud.cmq.client.netty;

public class RemoteConnectException extends RemoteException {

    private static final long serialVersionUID = -427307050745035434L;

    public RemoteConnectException(String address) {
        this(address, null);
    }

    public RemoteConnectException(String address, Throwable cause) {
        super("connect to <" + address + "> failed", cause);
    }
}
