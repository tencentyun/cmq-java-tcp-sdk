package com.qcloud.cmq.client.http;

import com.qcloud.cmq.client.common.LogHelper;
import com.qcloud.cmq.client.common.ResponseCode;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;
import org.slf4j.Logger;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import java.io.*;
import java.net.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

class HttpConnection {
    private final Logger logger = LogHelper.getLog();

    private int timeout;
    private boolean isKeepAlive;

    HttpConnection() {
        this.timeout = 10000;
        this.isKeepAlive = true;
    }

    private URLConnection newHttpConnection(String url) throws KeyManagementException, NoSuchAlgorithmException, IOException{
        trustAllHttpsCertificates();
        URLConnection connection;
        URL realUrl = new URL(url);
        if (url.toLowerCase().startsWith("https")) {
            HttpsURLConnection httpsConn = (HttpsURLConnection) realUrl.openConnection();
            httpsConn.setHostnameVerifier(new HostnameVerifier() {
                public boolean verify(String hostname, SSLSession session) {
                    return true;
                }
            });
            connection = httpsConn;
        } else {
            connection = realUrl.openConnection();
        }
        connection.setRequestProperty("Accept", "*/*");
        if (this.isKeepAlive) {
            connection.setRequestProperty("Connection", "Keep-Alive");
        }
        connection.setRequestProperty("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
        return connection;
    }

    String request(String method, String url, String req)
            throws MQClientException, MQServerException {
        StringBuilder result = new StringBuilder();
        BufferedReader in = null;
        try {
            URLConnection connection = this.newHttpConnection(url);
            connection.setConnectTimeout(timeout);

            if (method.equals("POST")) {
                ((HttpURLConnection) connection).setRequestMethod("POST");

                connection.setDoOutput(true);
                connection.setDoInput(true);
                DataOutputStream out = new DataOutputStream(connection.getOutputStream());
                out.writeBytes(req);
                out.flush();
                out.close();
            }

            connection.connect();
            int status = ((HttpURLConnection) connection).getResponseCode();
            if (status != 200) {
                String errorMsg = String.format("Http request error. return http code:%d", status);
                logger.error(errorMsg);
                throw new MQServerException(ResponseCode.HTTP_REQUEST_ERROR, errorMsg);
            }
            in = new BufferedReader(new InputStreamReader(connection.getInputStream(), "utf-8"));

            String line;
            while ((line = in.readLine()) != null) {
                result.append(line);
            }

        } catch (IOException e) {
            logger.error("call http request with IOException", e);
            throw new MQServerException(ResponseCode.HTTP_REQUEST_ERROR, "call http request error:" + e.getLocalizedMessage());
        } catch (NoSuchAlgorithmException e) {
            logger.error("call http request with signature error", e);
            throw new MQClientException("call http request with signature error", e);
        } catch (KeyManagementException e) {
            logger.error("call http request with signature error", e);
            throw new MQClientException("call http request with signature error", e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                // ignore
            }
        }
        return result.toString();
    }

    private static void trustAllHttpsCertificates() throws NoSuchAlgorithmException, KeyManagementException {
        javax.net.ssl.TrustManager[] trustAllCerts = new javax.net.ssl.TrustManager[1];
        javax.net.ssl.TrustManager tm = new miTM();
        trustAllCerts[0] = tm;
        javax.net.ssl.SSLContext sc = javax.net.ssl.SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, null);
        javax.net.ssl.HttpsURLConnection.setDefaultSSLSocketFactory(sc
                .getSocketFactory());
    }

    static class miTM implements javax.net.ssl.TrustManager, javax.net.ssl.X509TrustManager {
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return null;
        }

        public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType)
                throws java.security.cert.CertificateException {
        }

        public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType)
                throws java.security.cert.CertificateException {
        }
    }
}
