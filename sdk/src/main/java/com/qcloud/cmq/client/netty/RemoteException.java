package com.qcloud.cmq.client.netty;

public class RemoteException extends Exception {

    private static final long serialVersionUID = -6503868366712460484L;

    public RemoteException(String message) {
        super(message);
    }

    public RemoteException(String message, Throwable cause) {
        super(message, cause);
    }
}
