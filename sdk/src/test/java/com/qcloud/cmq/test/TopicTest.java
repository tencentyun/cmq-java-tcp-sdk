package com.qcloud.cmq.test;

import com.qcloud.cmq.client.cloudapi.CloudApiManager;
import com.qcloud.cmq.client.cloudapi.SubscribeConfig;
import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.common.CloudApiClientConfig;
import com.qcloud.cmq.client.consumer.Consumer;
import com.qcloud.cmq.client.consumer.ReceiveResult;
import com.qcloud.cmq.client.producer.Producer;
import com.qcloud.cmq.client.producer.PublishCallback;
import com.qcloud.cmq.client.producer.PublishResult;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author: feynmanlin
 * @date: 2020/1/20 11:44 上午
 */
public class TopicTest {
    static Producer producer = new Producer();
    static Consumer consumer = new Consumer();
    static CloudApiManager cloudApiManager = new CloudApiManager();
    static String queueName = "queue-test10" + System.currentTimeMillis();
    static String msg = "testMsg";
    static String secretId = "";
    static String secretKey = "";

    @BeforeClass
    public static void init() {

        producer.setNameServerAddress("http://cmq-nameserver-gz.tencentcloudapi.com");
        producer.setSecretId(secretId);
        producer.setSecretKey(secretKey);
        producer.setSignMethod(ClientConfig.SIGN_METHOD_SHA256);
        producer.setRetryTimesWhenSendFailed(3);
        producer.setRequestTimeoutMS(10000);
        producer.start();

        consumer.setNameServerAddress("http://cmq-nameserver-gz.tencentcloudapi.com");
        consumer.setSecretId(secretId);
        consumer.setSecretKey(secretKey);
        consumer.setSignMethod(ClientConfig.SIGN_METHOD_SHA256);
        consumer.setBatchPullNumber(10);
        consumer.setPollingWaitSeconds(10);
        consumer.setRequestTimeoutMS(10000);
        consumer.start();

        cloudApiManager.setCloudApiAddress("http://cmq-queue-gz.api.qcloud.com");
        cloudApiManager.setSecretId(secretId);
        cloudApiManager.setSecretKey(secretKey);
        cloudApiManager.setSignMethod(CloudApiClientConfig.SIGN_METHOD_SHA1);
        cloudApiManager.setConnectTimeout(5000);
        cloudApiManager.start();

        cloudApiManager.createQueue(queueName);
    }

    @AfterClass
    public static void cleaner() throws InterruptedException {
        Thread.sleep(10000);
        cloudApiManager.deleteQueue(queueName);
    }

    @Test
    public void testCreateAndDeleteSubscribe() throws InterruptedException {
        String topicName = "addddd";
        String subscribeName = "subscribeName";
        SubscribeConfig subscribeConfig = new SubscribeConfig();
        subscribeConfig.setTopicName(topicName);
        subscribeConfig.setProtocol("queue");
        subscribeConfig.setSubscriptionName(subscribeName);
        subscribeConfig.setEndpoint(queueName);
        subscribeConfig.setBindingKey(Arrays.asList("double"));

        cloudApiManager.createSubscribe(subscribeConfig);
        Thread.sleep(3000);

        producer.publish(topicName, msg);

        //wait for push
        Thread.sleep(3000);
        ReceiveResult receiveResult = consumer.receiveMsg(queueName);
        Assert.assertEquals(msg, receiveResult.getMessage().getData());

        AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        producer.publish(topicName, msg, new ArrayList<>(), new PublishCallback() {
            @Override
            public void onSuccess(PublishResult publishResult) {
                System.out.println(publishResult);
                atomicBoolean.set(true);
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {

            }
        });
        countDownLatch.await(10, TimeUnit.SECONDS);

        receiveResult = consumer.receiveMsg(queueName, 10);
        Assert.assertEquals(msg, receiveResult.getMessage().getData());

        Assert.assertEquals(true, atomicBoolean.get());

        Thread.sleep(5000);
        cloudApiManager.deleteSubscribe(topicName, subscribeName);
    }

}
