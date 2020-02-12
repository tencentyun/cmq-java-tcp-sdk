package com.qcloud.cmq.client.http;

import com.qcloud.cmq.client.common.LogHelper;
import com.qcloud.cmq.client.common.ResponseCode;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.TreeMap;

/**
 * @author: feynmanlin
 * @date: 2020/1/17 11:43 上午
 */
public abstract class AbstractHttpWrapper implements HttpWrapper{

    private final Logger logger = LogHelper.getLog();

    private String endpoint;
    private String path;
    private String secretId;
    private String secretKey;
    private String method;


    public AbstractHttpWrapper(String endpoint, String path, String secretId,
                               String secretKey, String method){
        this.endpoint = endpoint;
        this.path = path;
        this.secretId = secretId;
        this.secretKey = secretKey;
        this.method = method;
    }

    @Override
    public String call(String action, TreeMap<String, String> param) throws MQClientException, MQServerException {
        try {
            param.put("Action", action);
            param.put("Nonce", Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
            param.put("SecretId", this.secretId);
            param.put("Timestamp", Long.toString(System.currentTimeMillis() / 1000));
            addSignatureMethod(param);
            String host;
            if (this.endpoint.startsWith("https")) {
                host = this.endpoint.substring(8);
            } else {
                host = this.endpoint.substring(7);
            }
            StringBuilder src = new StringBuilder();
            src.append(this.method).append(host).append(this.path).append("?");

            boolean flag = false;
            for (String key : param.keySet()) {
                if (flag) {
                    src.append("&");
                }
                src.append(key.replace("_", ".")).append("=").append(param.get(key));
                flag = true;
            }
            addSignature(param, src.toString(), secretKey);
            StringBuilder url = new StringBuilder(this.endpoint + this.path);
            StringBuilder req = new StringBuilder();
            flag = false;
            for (String key : param.keySet()) {
                if (flag) {
                    req.append("&");
                }
                req.append(key).append("=").append(URLEncoder.encode(param.get(key), "utf-8"));
                flag = true;
            }
            if (this.method.equals("GET")) {
                url.append("?").append(req);
                req = new StringBuilder();
                if (url.length() > 2048) {
                    throw new MQClientException(ResponseCode.HTTP_GET_URL_TOO_LONG,
                            "URL length is larger than 2K when use GET method");
                }
            }
            if (LogHelper.LOG_REQUEST) {
                logger.debug("===> call param:" + param);
            }
            String result = doRequest(method, url.toString(), req.toString());
            if (LogHelper.LOG_REQUEST) {
                logger.debug("===> result:" + result);
            }
            return result;
        } catch (Exception e) {
            logger.error("call http request error", e);
            throw new MQClientException("call http request failed.", e);
        }
    }

    /**
     * do Request
     * @param method
     * @param url
     * @param requestParam
     * @return
     */
    protected abstract String doRequest(String method, String url, String requestParam);

    /**
     * add Signature to param
     * @param param
     * @param src
     * @param secretKey
     * @throws NoSuchAlgorithmException
     * @throws UnsupportedEncodingException
     * @throws InvalidKeyException
     */
    protected abstract void addSignature(TreeMap<String, String> param, String src, String secretKey) throws NoSuchAlgorithmException, UnsupportedEncodingException, InvalidKeyException;

    /**
     * add Signature Method to param
     * @param param
     */
    protected abstract void addSignatureMethod(TreeMap<String, String> param);
}
