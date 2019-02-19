package com.qcloud.cmq.client.exception;

public class MQServerException extends Exception {
    private static final long serialVersionUID = -2318482622777702963L;
    private final int responseCode;
    private final String errorMessage;

    public MQServerException(int responseCode, String errorMessage) {
        super("CODE:" + responseCode + ", DESC:" + errorMessage);
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
