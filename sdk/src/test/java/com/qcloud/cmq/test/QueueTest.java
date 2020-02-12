package com.qcloud.cmq.test;

import com.qcloud.cmq.client.cloudapi.CloudApiManager;
import com.qcloud.cmq.client.cloudapi.QueueMeta;
import com.qcloud.cmq.client.cloudapi.entity.CmqQueue;
import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.common.CloudApiClientConfig;
import com.qcloud.cmq.client.consumer.Consumer;
import com.qcloud.cmq.client.consumer.ReceiveResult;
import com.qcloud.cmq.client.producer.Producer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 * @author: feynmanlin
 * @date: 2020/1/20 10:23 上午
 */
public class QueueTest {
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
    public static void cleaner(){
        cloudApiManager.deleteQueue(queueName);
    }

    @Test
    public void testCreateQueue() throws InterruptedException {
        String newQueueName = queueName + System.currentTimeMillis();
        cloudApiManager.createQueue(newQueueName);
        Thread.sleep(2000);

        int count = cloudApiManager.countQueue(newQueueName, 0, 10);
        Assert.assertEquals(1, count);

        List<CmqQueue> queueList = cloudApiManager.describeQueue(newQueueName, 0, 10);
        Assert.assertTrue(queueList != null && queueList.size() == 1);
        Assert.assertEquals(newQueueName, queueList.get(0).getQueueName());

        producer.send(newQueueName, msg);
        Thread.sleep(3000);
        ReceiveResult receiveResult = consumer.receiveMsg(newQueueName,20);
        Assert.assertEquals(receiveResult.getMessage().getData(), msg);
        consumer.deleteMsg(newQueueName, receiveResult.getMessage().getReceiptHandle());
        Thread.sleep(3000);
        cloudApiManager.deleteQueue(newQueueName);
    }

    @Test
    public void testCreateQueueWithMata() throws InterruptedException {
        QueueMeta queueMeta = new QueueMeta();
        queueMeta.setMaxMsgSize(2048);
        queueMeta.setVisibilityTimeout(100);
        queueMeta.setMaxMsgHeapNum(1000001);
        queueMeta.setMsgRetentionSeconds(60);
        queueMeta.setPollingWaitSeconds(30);
        String newQueueName = queueName + System.currentTimeMillis();
        cloudApiManager.createQueue(newQueueName, queueMeta);
        Thread.sleep(2000);

        int count = cloudApiManager.countQueue(newQueueName, 0, 10);
        Assert.assertEquals(1, count);

        List<CmqQueue> queueList = cloudApiManager.describeQueue(newQueueName, 0, 10);
        Assert.assertTrue(queueList != null && queueList.size() == 1);
        Assert.assertEquals(newQueueName, queueList.get(0).getQueueName());

        //Describe Queue Attributes And Assert
        QueueMeta queueMeta1 = cloudApiManager.describeQueueAttributes(newQueueName);
        Assert.assertEquals(queueMeta.getMaxMsgSize(), queueMeta1.getMaxMsgSize());
        Assert.assertEquals(queueMeta.getVisibilityTimeout(), queueMeta1.getVisibilityTimeout());
        Assert.assertEquals(queueMeta.getMaxMsgHeapNum(), queueMeta1.getMaxMsgHeapNum());
        Assert.assertEquals(queueMeta.getMsgRetentionSeconds(), queueMeta1.getMsgRetentionSeconds());
        Assert.assertEquals(queueMeta.getPollingWaitSeconds(), queueMeta1.getPollingWaitSeconds());

        producer.send(newQueueName, msg);
        ReceiveResult receiveResult = consumer.receiveMsg(newQueueName,10);
        consumer.deleteMsg(newQueueName, receiveResult.getMessage().getReceiptHandle());
        Assert.assertEquals(receiveResult.getMessage().getData(), msg);

        Thread.sleep(5000);
        cloudApiManager.deleteQueue(newQueueName);

        count = cloudApiManager.countQueue(newQueueName, 0, 10);
        Assert.assertEquals(0, count);
    }

    @Test
    public void testCreateAndSubscribe() throws InterruptedException {
        String newQueueName = queueName + System.currentTimeMillis();
        String topicName = "addddd";
        cloudApiManager.createQueueAndSubscribe(newQueueName, "addddd", "addddd-Subscribe");
        Thread.sleep(3000);
        producer.publish(topicName, msg);
        Thread.sleep(3000);
        ReceiveResult receiveResult = consumer.receiveMsg(newQueueName,10);
        Assert.assertEquals(msg, receiveResult.getMessage().getData());
        Thread.sleep(5000);
        cloudApiManager.deleteSubscribe(topicName,"addddd-Subscribe");
    }

}
