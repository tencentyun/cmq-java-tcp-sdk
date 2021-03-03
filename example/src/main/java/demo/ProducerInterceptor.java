package demo;

import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.common.ResponseCode;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;
import com.qcloud.cmq.client.producer.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ProducerInterceptor {

    public static void main(String args[]) {

        Interceptor interceptor = new Interceptor();

        Producer producer = new Producer(Arrays.asList(interceptor));
        // 设置 Name Server地址。必须设置 不同地域不同网络不同
        // 地域对应的缩写 bj:北京 sh：上海 gz:广州 in:孟买 ca:北美 cd:成都 cq: 重庆
        //  hk:香港 kr:韩国 ru:俄罗斯 sg:新加坡 shjr:上海金融 szjr:深圳金融 th:曼谷 use: 弗吉尼亚 usw： 美西
        // 私有网络地址：http://cmq-nameserver-vpc-{region}.api.tencentyun.com 支持腾讯云私有网络的云服务器内网访问
        // 公网地址：    http://cmq-nameserver-{region}.tencentcloudapi.com
        producer.setNameServerAddress("http://cmq-nameserver-gz.tencentcloudapi.com");
        // 设置SecretId，在控制台上获取，必须设置
        producer.setSecretId("xxx");
        // 设置SecretKey，在控制台上获取，必须设置
        producer.setSecretKey("xxx");
        // 设置签名方式，可以不设置，默认为SHA1
        producer.setSignMethod(ClientConfig.SIGN_METHOD_SHA256);
        // 设置发送消息失败时，重试的次数，设置为0表示不重试，默认为2
        producer.setRetryTimesWhenSendFailed(3);
        // 设置请求超时时间， 默认3000ms
        producer.setRequestTimeoutMS(5000);

        // 消息发往的队列，在控制台创建
        String queue = "queue-test10";
        try {
            // 启动对象前必须设置好相关参数
            producer.start();
            final String msg = "tre张测试";
            // 同步单条发送消息
            SendResult result = producer.send(queue, msg);
            if (result.getReturnCode() == ResponseCode.SUCCESS) {
                System.out.println("==> send success! msg_id:" + result.getMsgId() + " request_id:" + result.getRequestId());
            } else {
                System.out.println("==> code:" + result.getReturnCode() + " error:" + result.getErrorMsg());
            }

            // 同步单条发送消息，设置延迟8s，8s后可消费到
            result = producer.send(queue, msg, 2);
            if (result.getReturnCode() == ResponseCode.SUCCESS) {
                System.out.println("==> send success! msg_id:" + result.getMsgId() + " request_id:" + result.getRequestId());
            } else {
                System.out.println("==> code:" + result.getReturnCode() + " error:" + result.getErrorMsg());
            }

            // 异步发送消息
            producer.send(queue, msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    if (sendResult.getReturnCode() == ResponseCode.SUCCESS) {
                        System.out.println("==> send success! msg_id:" + sendResult.getMsgId() + " request_id:" + sendResult.getRequestId());
                    } else {
                        System.out.println("==> code:" + sendResult.getReturnCode() + " error:" + sendResult.getErrorMsg());
                    }
                }

                @Override
                public void onException(Throwable e) {
                    e.printStackTrace();
                    System.out.println("==> send error: " + e);
                }
            });

            // 批量发送消息
            final List<String> msgList = new ArrayList<String>();
            msgList.add("hello");
            msgList.add("world");
            // 同步批量发送消息
            BatchSendResult batchSendResult = producer.batchSend(queue, msgList);
            if (batchSendResult.getReturnCode() == ResponseCode.SUCCESS) {
                System.out.println("==> send success! msg_id:" + batchSendResult.getMsgIdList() + " request_id:" + result.getRequestId());
            } else {
                System.out.println("==> code:" + batchSendResult.getReturnCode() + " error:" + batchSendResult.getErrorMessage());
            }

            // 异步批量发送，并且延迟5s
            producer.batchSend(queue, msgList, 5, new BatchSendCallback() {
                @Override
                public void onSuccess(BatchSendResult result) {
                    if (result.getReturnCode() == ResponseCode.SUCCESS) {
                        System.out.println("==> send success! msg_id:" + result.getMsgIdList() + " request_id:" + result.getRequestId());
                    } else {
                        System.out.println("==> code:" + result.getReturnCode() + " error:" + result.getErrorMessage());
                    }
                }
                @Override
                public void onException(Throwable e) {
                    e.printStackTrace();
                    System.out.println("==> send error: " + e);
                }
            });

        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (MQServerException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(5000);
            producer.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
