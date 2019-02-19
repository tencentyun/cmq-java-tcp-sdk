package com.qcloud.cmq.client.netty;

public class RemoteTimeoutException extends RemoteException {

    private static final long serialVersionUID = 5143379248697371924L;

    public RemoteTimeoutException(String message) {
        super(message);
    }

    public RemoteTimeoutException(String address, long timeoutMillis) {
        this(address, timeoutMillis, null);
    }

    public RemoteTimeoutException(String address, long timeoutMillis, Throwable cause) {
        super("wait response on the channel <" + address + "> timeout, " + timeoutMillis + "(ms)", cause);
    }
}
