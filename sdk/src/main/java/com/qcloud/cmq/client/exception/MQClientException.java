package com.qcloud.cmq.client.exception;


import com.qcloud.cmq.client.common.ResponseCode;

public class MQClientException extends Exception {
    private static final long serialVersionUID = -8332788844751379650L;
    private int responseCode;
    private String errorMessage;

    public MQClientException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
        this.responseCode = ResponseCode.SYSTEM_ERROR;
        this.errorMessage = errorMessage;
    }

    public MQClientException(int responseCode, String errorMessage) {
        super("CODE:" + responseCode + ", DESC:" + errorMessage);
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public MQClientException setResponseCode(final int responseCode) {
        this.responseCode = responseCode;
        return this;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(final String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
