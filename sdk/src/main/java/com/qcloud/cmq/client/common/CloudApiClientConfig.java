package com.qcloud.cmq.client.common;


import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

/**
 * @author feynmanlin
 */
public class CloudApiClientConfig {
    public static final String DEFAULT_PATH = "/v2/index.php";

    private static final String CLOUD_API_ADDR_PROPERTY = "cmq.cloud.addr";

    private static final String CMQ_SECRET_ID_ENV = "CMQ_SECRET_ID";
    private static final String CMQ_SECRET_ID_PROPERTY = "cmq.client.secretId";
    private static final String CMQ_SECRET_KEY_ENV = "CMQ_SECRET_KEY";
    private static final String CMQ_SECRET_KEY_PROPERTY = "cmq.client.secretKey";
    private static final String CMQ_SIGN_METHOD_ENV = "CMQ_SIGN_METHOD";
    private static final String CMQ_SIGN_METHOD_PROPERTY = "cmq.client.signMethod";

    public static final String SIGN_METHOD_SHA1 = "sha1";
    public static final String SIGN_METHOD_SHA256 = "sha256";

    private String cloudApiAddress = System.getProperty(CLOUD_API_ADDR_PROPERTY, System.getenv(CLOUD_API_ADDR_PROPERTY));


    private String secretId = System.getProperty(CMQ_SECRET_ID_PROPERTY, System.getenv(CMQ_SECRET_ID_ENV));
    private String secretKey = System.getProperty(CMQ_SECRET_KEY_PROPERTY, System.getenv(CMQ_SECRET_KEY_ENV));
    private String signMethod = System.getProperty(CMQ_SIGN_METHOD_PROPERTY, System.getenv(CMQ_SIGN_METHOD_ENV));

    private int requestTimeoutMS = 3000;
    private int connectTimeout = 10000;


    public CloudApiClientConfig cloneClientConfig() {
        CloudApiClientConfig cc = new CloudApiClientConfig();
        cc.secretId = secretId;
        cc.secretKey = secretKey;
        cc.signMethod = signMethod;
        cc.requestTimeoutMS = requestTimeoutMS;
        cc.cloudApiAddress = cloudApiAddress;
        cc.connectTimeout = connectTimeout;
        return cc;
    }


    public String getCloudApiAddress() {
        return cloudApiAddress;
    }

    public void setCloudApiAddress(String cloudApiAddress) {
        this.cloudApiAddress = cloudApiAddress;
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

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    @Override
    public String toString() {
        return "CloudApiClientConfig{" +
                "cloudApiAddress='" + cloudApiAddress + '\'' +
                ", secretId='" + secretId + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", signMethod='" + signMethod + '\'' +
                ", requestTimeoutMS=" + requestTimeoutMS +
                ", connectTimeout=" + connectTimeout +
                '}';
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
