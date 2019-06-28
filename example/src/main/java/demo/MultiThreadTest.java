package demo;

import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;
import com.qcloud.cmq.client.producer.Producer;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.concurrent.*;

public class MultiThreadTest {
    public static void main(String[] args) throws InterruptedException, MQClientException {
        Producer producer = new Producer();
        // 设置 Name Server地址。必须设置 不同地域不同网络不同 
        // 地域对应的缩写 bj:北京 sh：上海 gz:广州 in:孟买 ca:北美 cd:成都 cq: 重庆
        //  hk:香港 kr:韩国 ru:俄罗斯 sg:新加坡 shjr:上海金融 szjr:深圳金融 th:曼谷 use: 弗吉尼亚 usw： 美西 
        // 私有网络地址：http://cmq-nameserver-vpc-{region}.api.tencentyun.com 支持腾讯云私有网络的云服务器内网访问
        // 公网地址：    http://cmq-nameserver-{region}.tencentcloudapi.com
        producer.setNameServerAddress("http://cmq-nameserver-xxx.tencentcloudapi.com");

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

        producer.start();
        CountDownLatch latch = new CountDownLatch(100);

        BlockingQueue queue = new ArrayBlockingQueue(100);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(100, 100, 60, TimeUnit.SECONDS, queue);

        String topic = "test-group";
        String msg = "msg-test-group";

        for (int i = 0; i < 100; i++) {
            threadPoolExecutor.submit(() -> {
                try {
                    latch.await();
                    for (int j = 0; j < 5000; j++) {
                        producer.publish(topic, msg, Collections.singletonList("test-tag"));
                    }
                    System.out.println("send over ! " + Thread.currentThread());
                } catch (InterruptedException | MQClientException | MQServerException e) {
                    e.printStackTrace();
                }
            });
            latch.countDown();
        }

        Thread.sleep(60000);
        producer.shutdown();
    }
}
