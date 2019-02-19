package com.qcloud.cmq.client.common;


import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

public class ClientConfig {
    private static final String NAMESRV_ADDR_ENV = "NAMESRV_ADDR";
    private static final String NAMESRV_ADDR_PROPERTY = "cmq.namesrv.addr";
    private static final String CLIENT_NAME_PROPERTY = "cmq.client.name";

    private static final String CMQ_SECRET_ID_ENV = "CMQ_SECRET_ID";
    private static final String CMQ_SECRET_ID_PROPERTY = "cmq.client.secretId";
    private static final String CMQ_SECRET_KEY_ENV = "CMQ_SECRET_KEY";
    private static final String CMQ_SECRET_KEY_PROPERTY = "cmq.client.secretKey";
    private static final String CMQ_SIGN_METHOD_ENV = "CMQ_SIGN_METHOD";
    private static final String CMQ_SIGN_METHOD_PROPERTY = "cmq.client.signMethod";

    public static final String SIGN_METHOD_SHA1 = "HmacSHA1";
    public static final String SIGN_METHOD_SHA256 = "HmacSHA256";

    private String nameServerAddress = System.getProperty(NAMESRV_ADDR_PROPERTY, System.getenv(NAMESRV_ADDR_ENV));
    private String clientIP = RemoteHelper.getLocalAddress();
    private String instanceName = System.getProperty(CLIENT_NAME_PROPERTY, "DEFAULT");
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();

    private String secretId = System.getProperty(CMQ_SECRET_ID_PROPERTY, System.getenv(CMQ_SECRET_ID_ENV));
    private String secretKey = System.getProperty(CMQ_SECRET_KEY_PROPERTY, System.getenv(CMQ_SECRET_KEY_ENV));
    private String signMethod = System.getProperty(CMQ_SIGN_METHOD_PROPERTY, System.getenv(CMQ_SIGN_METHOD_ENV));

    private int requestTimeoutMS = 3000;
    private int retryTimesWhenSendFailed = 2;
    private int pollingWaitSeconds = 10;
    private int batchPullNumber = 16;

    public String buildMQClientId() {
        return this.getClientIP() + "@" + this.getInstanceName();
    }

    public ClientConfig cloneClientConfig() {
        ClientConfig cc = new ClientConfig();
        cc.nameServerAddress = nameServerAddress;
        cc.clientIP = clientIP;
        cc.instanceName = instanceName;
        cc.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
        cc.secretId = secretId;
        cc.secretKey = secretKey;
        cc.signMethod = signMethod;
        cc.requestTimeoutMS = requestTimeoutMS;
        cc.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
        cc.pollingWaitSeconds = pollingWaitSeconds;
        cc.batchPullNumber = batchPullNumber;
        return cc;
    }

    public void changeInstanceNameToPID() {
        this.instanceName = String.valueOf(getPid());
    }

    public String getNameServerAddress() {
        return nameServerAddress;
    }

    public void setNameServerAddress(String nameServerAddress) {
        this.nameServerAddress = nameServerAddress;
    }

    public String getClientIP() {
        return clientIP;
    }

    public void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }

    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }

    public String getSecretId() {
        return secretId;
    }

    public void setSecretId(String secretId) {
        this.secretId = secretId;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getSignMethod() {
        return signMethod;
    }

    public void setSignMethod(String signMethod) {
        this.signMethod = signMethod;
    }

    public int getRequestTimeoutMS() {
        return requestTimeoutMS;
    }

    public void setRequestTimeoutMS(int requestTimeoutMS) {
        this.requestTimeoutMS = requestTimeoutMS;
    }

    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    public int getPollingWaitSeconds() {
        return pollingWaitSeconds;
    }

    public void setPollingWaitSeconds(int pollingWaitSeconds) {
        this.pollingWaitSeconds = pollingWaitSeconds;
    }

    public int getBatchPullNumber() {
        return batchPullNumber;
    }

    public void setBatchPullNumber(int batchPullNumber) {
        this.batchPullNumber = batchPullNumber;
    }

    @Override
    public String toString() {
        return "ClientConfig[nameServerAddress=" + nameServerAddress + ", clientIP=" + clientIP + ", instanceName=" + instanceName
                + ", clientCallbackExecutorThreads=" + clientCallbackExecutorThreads + ", secretId=" + secretId
                + ", secretKey=" + secretKey + ", signMethod=" + (signMethod == null? SIGN_METHOD_SHA1: signMethod)
                + ", requestTimeoutMS=" + requestTimeoutMS + ", retryTimesWhenSendFailed=" + retryTimesWhenSendFailed
                + ", pollingWaitSeconds=" + pollingWaitSeconds + ", batchPullNumber=" + batchPullNumber + "]";
    }

    private static int getPid() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        String name = runtime.getName(); // format: "pid@hostname"
        try {
            return Integer.parseInt(name.substring(0, name.indexOf('@')));
        } catch (Exception e) {
            return -1;
        }
    }
}
