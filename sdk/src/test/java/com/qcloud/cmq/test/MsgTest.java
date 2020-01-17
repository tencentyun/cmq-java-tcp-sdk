package com.qcloud.cmq.test;

import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.consumer.Consumer;
import com.qcloud.cmq.client.producer.Producer;
import org.junit.Before;
import org.junit.Test;

/**
 * @author: feynmanlin
 * @date: 2020/1/17 3:03 下午
 */

public class MsgTest {
    Producer producer = new Producer();
    Consumer consumer = new Consumer();

    @Before
    public void init(){
        producer.setNameServerAddress("http://cmq-queue-gz.api.qcloud.com");
        producer.setSecretId("");
        producer.setSecretKey("");
        producer.setSignMethod(ClientConfig.SIGN_METHOD_SHA256);
        producer.setRetryTimesWhenSendFailed(3);
        producer.setRequestTimeoutMS(5000);
        producer.start();

        /*consumer.setNameServerAddress("http://cmq-queue-gz.api.qcloud.com");
        consumer.setSecretId("");
        consumer.setSecretKey("");
        consumer.setSignMethod(ClientConfig.SIGN_METHOD_SHA256);
        consumer.setBatchPullNumber(10);
        consumer.setPollingWaitSeconds(6);
        consumer.setRequestTimeoutMS(5000);
        consumer.start();*/
    }

    @Test
    public void testSingleMessge(){
        producer.send("queue-test10","testMsg");

    }
}
