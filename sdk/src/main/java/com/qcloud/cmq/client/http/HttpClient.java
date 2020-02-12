package com.qcloud.cmq.client.http;

import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.common.SignTool;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.TreeMap;

public class HttpClient extends AbstractHttpWrapper {

    private String signMethod;
    private HttpConnection httpConnection;

    public HttpClient(String endpoint, String path, String secretId,
                            String secretKey, String method, String signMethod) {
        super(endpoint, path, secretId, secretKey, method);
        this.signMethod = signMethod;
        this.httpConnection = new HttpConnection();
    }

    @Override
    protected String doRequest(String method, String url, String requestParam) {
        return this.httpConnection.request(method, url, requestParam);
    }

    @Override
    protected void addSignature(TreeMap<String, String> param, String src, String secretKey) throws NoSuchAlgorithmException, UnsupportedEncodingException, InvalidKeyException {
        param.put("Signature", SignTool.sign(src, secretKey, signMethod));
    }

    @Override
    protected void addSignatureMethod(TreeMap<String, String> param) {
        if (this.signMethod == null) {
            param.put("SignatureMethod", ClientConfig.SIGN_METHOD_SHA1);
        } else {
            param.put("SignatureMethod", this.signMethod);
        }
    }
}
