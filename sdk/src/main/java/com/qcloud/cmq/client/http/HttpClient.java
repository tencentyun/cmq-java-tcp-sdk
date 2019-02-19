package com.qcloud.cmq.client.http;

import com.qcloud.cmq.client.common.*;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.TreeMap;

public class HttpClient {
    private final Logger logger = LogHelper.getLog();

    private String endpoint;
    private String path;
    private String secretId;
    private String secretKey;
    private String method;
    private String signMethod;
    private HttpConnection httpConnection;

    public HttpClient(String endpoint, String path, String secretId,
                      String secretKey, String method, String signMethod) {
        this.endpoint = endpoint;
        this.path = path;
        this.secretId = secretId;
        this.secretKey = secretKey;
        this.method = method;
        this.signMethod = signMethod;
        this.httpConnection = new HttpConnection();
    }

    public String call(String action, TreeMap<String, String> param) throws MQClientException, MQServerException {
        try {
            param.put("Action", action);
            param.put("Nonce", Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
            param.put("SecretId", this.secretId);
            param.put("Timestamp", Long.toString(System.currentTimeMillis() / 1000));
            if (this.signMethod == null) {
                param.put("SignatureMethod", ClientConfig.SIGN_METHOD_SHA1);
            } else {
                param.put("SignatureMethod", this.signMethod);
            }
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
            param.put("Signature", SignTool.sign(src.toString(), this.secretKey, this.signMethod));
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
            String result = this.httpConnection.request(this.method, url.toString(), req.toString());
            if (LogHelper.LOG_REQUEST) {
                logger.debug("===> result:" + result);
            }
            return result;
        } catch (NoSuchAlgorithmException e) {
            logger.error("call http request error", e);
            throw new MQClientException("call http request failed.", e);
        } catch (UnsupportedEncodingException e) {
            logger.error("call http request error", e);
            throw new MQClientException("call http request failed.", e);
        } catch (InvalidKeyException e) {
            logger.error("call http request error", e);
            throw new MQClientException("call http request failed.", e);
        }
    }
}
