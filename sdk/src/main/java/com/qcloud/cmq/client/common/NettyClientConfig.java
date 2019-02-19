package com.qcloud.cmq.client.common;


public class NettyClientConfig {

    public static final String COM_CMQ_NETTY_SOCKET_SNDBUF_SIZE = "cmq.netty.socket.sndbuf.size";
    public static final String COM_CMQ_NETTY_SOCKET_RCVBUF_SIZE = "cmq.netty.socket.rcvbuf.size";
    public static final String COM_CMQ_NETTY_CLIENT_ASYNC_SEMAPHORE_VALUE = "cmq.netty.clientAsyncSemaphoreValue";
    public static final String COM_CMQ_NETTY_CLIENT_ONEWAY_SEMAPHORE_VALUE = "cmq.netty.clientOnewaySemaphoreValue";

    /**
     * Worker thread number
     */
    private int clientWorkerThreads = 4;
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    private int clientOnewaySemaphoreValue = Integer.parseInt(
            System.getProperty(COM_CMQ_NETTY_CLIENT_ONEWAY_SEMAPHORE_VALUE, "65535"));
    private int clientAsyncSemaphoreValue = Integer.parseInt(
            System.getProperty(COM_CMQ_NETTY_CLIENT_ASYNC_SEMAPHORE_VALUE, "65535"));
    private int connectTimeoutMillis = 3000;

    /**
     * IdleStateEvent will be triggered when neither read nor write was performed for
     * the specified period of this time. Specify {@code 0} to disable
     */
    private int clientChannelMaxIdleTimeSeconds = 3;

    private int clientSocketSndBufSize = Integer.parseInt(
            System.getProperty(COM_CMQ_NETTY_SOCKET_SNDBUF_SIZE, "65535"));
    private int clientSocketRcvBufSize = Integer.parseInt(
            System.getProperty(COM_CMQ_NETTY_SOCKET_RCVBUF_SIZE, "65535"));
    private boolean clientCloseSocketIfTimeout = false;

    public int getClientWorkerThreads() {
        return clientWorkerThreads;
    }

    public void setClientWorkerThreads(int clientWorkerThreads) {
        this.clientWorkerThreads = clientWorkerThreads;
    }


    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }

    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }

    public int getClientOnewaySemaphoreValue() {
        return clientOnewaySemaphoreValue;
    }

    public void setClientOnewaySemaphoreValue(int clientOnewaySemaphoreValue) {
        this.clientOnewaySemaphoreValue = clientOnewaySemaphoreValue;
    }

    public int getClientAsyncSemaphoreValue() {
        return clientAsyncSemaphoreValue;
    }

    public void setClientAsyncSemaphoreValue(int clientAsyncSemaphoreValue) {
        this.clientAsyncSemaphoreValue = clientAsyncSemaphoreValue;
    }

    public int getClientChannelMaxIdleTimeSeconds() {
        return clientChannelMaxIdleTimeSeconds;
    }

    public void setClientChannelMaxIdleTimeSeconds(int clientChannelMaxIdleTimeSeconds) {
        this.clientChannelMaxIdleTimeSeconds = clientChannelMaxIdleTimeSeconds;
    }

    public int getClientSocketSndBufSize() {
        return clientSocketSndBufSize;
    }

    public void setClientSocketSndBufSize(int clientSocketSndBufSize) {
        this.clientSocketSndBufSize = clientSocketSndBufSize;
    }

    public int getClientSocketRcvBufSize() {
        return clientSocketRcvBufSize;
    }

    public void setClientSocketRcvBufSize(int clientSocketRcvBufSize) {
        this.clientSocketRcvBufSize = clientSocketRcvBufSize;
    }

    public boolean isClientCloseSocketIfTimeout() {
        return clientCloseSocketIfTimeout;
    }

    public void setClientCloseSocketIfTimeout(final boolean clientCloseSocketIfTimeout) {
        this.clientCloseSocketIfTimeout = clientCloseSocketIfTimeout;
    }
}
