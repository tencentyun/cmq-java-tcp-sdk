package com.qcloud.cmq.client.common;

public class ResponseCode {

    public static final int SUCCESS = 0;

    public static final int SYSTEM_ERROR = 1;

    // Client Error
    public static final int SERVICE_STATE_ERROR = 10;
    public static final int ALREADY_SUBSCRIBED = 11;

    // Network Error
    public static final int SEND_REQUEST_ERROR = 20;

    // message
    public static final int MESSAGE_SIZE_TOO_LARGE = 4400; // => from server to client

    // http
    public static final int HTTP_GET_URL_TOO_LONG = 220;
    public static final int SIGNATURE_METHOD_NOT_SUPPORT = 221;
    public static final int HTTP_REQUEST_ERROR = 223;

    public static final int PRODUCE_MESSAGE_TOO_QUICK = 221;

    // name server
    public static final int ROUTE_NOT_FOUND = 4040;

    public static final int NO_NEW_MESSAGES = 10200;

}
