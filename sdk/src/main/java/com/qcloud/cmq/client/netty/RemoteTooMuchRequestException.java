package com.qcloud.cmq.client.netty;

public class RemoteTooMuchRequestException extends RemoteException {

    private static final long serialVersionUID = 4997928479105456037L;

    public RemoteTooMuchRequestException(String message) {
        super(message);
    }
}
