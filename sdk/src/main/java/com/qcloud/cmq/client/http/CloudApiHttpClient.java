package com.qcloud.cmq.client.http;

import com.qcloud.cmq.client.common.CMQTool;
import com.qcloud.cmq.client.common.CloudApiClientConfig;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.TreeMap;

/**
 * @author: feynmanlin
 * @date: 2020/1/16 6:01 下午
 */
public class CloudApiHttpClient extends AbstractHttpWrapper {

    private CloudApiClientConfig cloudApiClientConfig;
    private HttpConnection httpConnection;

    public CloudApiHttpClient(CloudApiClientConfig cloudApiClientConfig) {
        super(cloudApiClientConfig.getCloudApiAddress(), CloudApiClientConfig.DEFAULT_PATH
                , cloudApiClientConfig.getSecretId(), cloudApiClientConfig.getSecretKey(), "POST");
        this.cloudApiClientConfig = cloudApiClientConfig;
        httpConnection = new HttpConnection(cloudApiClientConfig.getRequestTimeoutMS(), false);
    }

    @Override
    protected String doRequest(String method, String url, String requestParam) {
        return httpConnection.request(method, url, requestParam);
    }

    @Override
    protected void addSignature(TreeMap<String, String> param, String src, String secretKey) throws NoSuchAlgorithmException, UnsupportedEncodingException, InvalidKeyException {
        param.put("Signature", CMQTool.sign(src, secretKey, cloudApiClientConfig.getSignMethod()));
    }

    @Override
    protected void addSignatureMethod(TreeMap<String, String> param) {
        if (CloudApiClientConfig.SIGN_METHOD_SHA1.equals(cloudApiClientConfig.getSignMethod())) {
            param.put("SignatureMethod", CloudApiClientConfig.SIGN_METHOD_SHA1);
        } else {
            param.put("SignatureMethod", CloudApiClientConfig.SIGN_METHOD_SHA256);
        }

    }
}
