package com.qcloud.cmq.test;

import com.qcloud.cmq.client.cloudapi.CloudApiManager;
import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.common.CloudApiClientConfig;
import com.qcloud.cmq.client.consumer.*;
import com.qcloud.cmq.client.producer.Producer;
import com.qcloud.cmq.client.producer.SendCallback;
import com.qcloud.cmq.client.producer.SendResult;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: feynmanlin
 * @date: 2020/1/17 3:03 下午
 */
@FixMethodOrder(MethodSorters.JVM)
public class MessageTest {
    static Producer producer = new Producer();
    static Consumer consumer = new Consumer();
    static CloudApiManager cloudApiManager = new CloudApiManager();
    static String queueName = "queue-test10" + System.currentTimeMillis();
    static String msg = "testMsg";
    static String secretId = "";
    static String secretKey = "";

    @BeforeClass
    public static void init() throws InterruptedException {


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
        Thread.sleep(5000);

    }

    @AfterClass
    public static void cleaner() {
        cloudApiManager.deleteQueue(queueName);
    }

    @Test
    public void testSendMessage() {
        producer.send(queueName, msg);
        ReceiveResult receiveResult = consumer.receiveMsg(queueName);
        consumer.deleteMsg(queueName, receiveResult.getMessage().getReceiptHandle());
        Assert.assertEquals(msg, receiveResult.getMessage().getData());

    }

    @Test
    public void testDelay() {
        producer.send(queueName, msg, 5);
        long start = System.currentTimeMillis();
        ReceiveResult receiveResult = consumer.receiveMsg(queueName);
        long end = System.currentTimeMillis();
        consumer.deleteMsg(queueName, receiveResult.getMessage().getReceiptHandle());
        Assert.assertTrue("actual:" + (end - start), end - start > 4000);
        Assert.assertEquals("actual:" + receiveResult.getMessage(), msg, receiveResult.getMessage().getData());

        producer.send(queueName, msg, 5);
        producer.send(queueName, msg, 5);
        producer.send(queueName, msg, 5);
        start = System.currentTimeMillis();
        BatchReceiveResult batchReceiveResult = consumer.batchReceiveMsg(queueName, 10);
        List<Message> messages = batchReceiveResult.getMessageList();
        Assert.assertTrue(messages != null && messages.size() == 3);

        long cost = System.currentTimeMillis() - start;
        Assert.assertTrue("actual:" + cost, cost > 4000);
        for (Message message : messages) {
            Assert.assertTrue(msg.equals(message.getData()));
            consumer.deleteMsg(queueName, message.getReceiptHandle());
        }
    }

    @Test
    public void testWithCallBack() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicBoolean flag = new AtomicBoolean(false);
        long start = System.currentTimeMillis();
        producer.send(queueName, msg, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                flag.set(true);
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                flag.set(true);
                countDownLatch.countDown();
            }
        });
        countDownLatch.await(20, TimeUnit.SECONDS);
        Assert.assertEquals(true, flag.get());
        ReceiveResult receiveResult = consumer.receiveMsg(queueName);
        long cost = System.currentTimeMillis() - start;
        consumer.deleteMsg(queueName, receiveResult.getMessage().getReceiptHandle());
        Assert.assertEquals("actual:" + receiveResult.getMessage(), msg, receiveResult.getMessage().getData());
        Assert.assertTrue("actual:" + cost, cost < 5000 && cost > 1000);
    }

    @Test
    public void testWithDelayAndCallBack() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicBoolean flag = new AtomicBoolean(false);
        long start = System.currentTimeMillis();
        producer.send(queueName, msg, 5, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                flag.set(true);
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                flag.set(true);
                countDownLatch.countDown();
            }
        });
        countDownLatch.await(20, TimeUnit.SECONDS);

        Assert.assertEquals(true, flag.get());
        ReceiveResult receiveResult = consumer.receiveMsg(queueName);
        long cost = System.currentTimeMillis() - start;
        consumer.deleteMsg(queueName, receiveResult.getMessage().getReceiptHandle());
        Assert.assertTrue("actual:" + cost, cost > 4000);
    }

    @Test
    public void testReceiveMessage() {

        long start = System.currentTimeMillis();
        ReceiveResult receiveResult = consumer.receiveMsg(queueName, 5);
        consumer.deleteMsg(queueName, receiveResult.getMessage().getReceiptHandle());
        long cost = System.currentTimeMillis() - start;
        Assert.assertEquals("", receiveResult.getMessage().getData());
        Assert.assertTrue(cost > 5000);

        start = System.currentTimeMillis();
        producer.send(queueName, msg, 4);
        receiveResult = consumer.receiveMsg(queueName, 10);
        consumer.deleteMsg(queueName, receiveResult.getMessage().getReceiptHandle());
        cost = System.currentTimeMillis() - start;
        Assert.assertTrue("actual:" + cost, cost > 3000 && cost < 10000);

    }

    @Test
    public void testReceiveCallbackMessage() throws InterruptedException {
        final AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(1);

        consumer.receiveMsg(queueName, 20,new ReceiveCallback() {
            @Override
            public void onSuccess(ReceiveResult receiveResult) {
                consumer.deleteMsg(queueName, receiveResult.getMessage().getReceiptHandle());
                counter.incrementAndGet();
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                throw new RuntimeException(e);
            }
        });
        producer.send(queueName, msg);

        countDownLatch.await(20, TimeUnit.SECONDS);

        Assert.assertTrue("actual:" + counter.get(), counter.get() == 1);
    }

    @Test
    public void testReceiveCallbackMessageWithPolling() throws InterruptedException {
        final AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        long start = System.currentTimeMillis();
        consumer.receiveMsg(queueName, 5, new ReceiveCallback() {
            @Override
            public void onSuccess(ReceiveResult receiveResult) {
                consumer.deleteMsg(queueName, receiveResult.getMessage().getReceiptHandle());
                counter.incrementAndGet();
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                throw new RuntimeException(e);
            }
        });

        countDownLatch.await(10, TimeUnit.SECONDS);
        long cost = System.currentTimeMillis() - start;

        Assert.assertTrue("actual:" + counter.get(), counter.get() == 1);
        Assert.assertTrue("actual:" + cost, cost >= 4000);
    }

    @Test
    public void testBatchReceive() throws InterruptedException {
        producer.send(queueName, msg);
        producer.send(queueName, msg);
        producer.send(queueName, msg);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        consumer.batchReceiveMsg(queueName, new BatchReceiveCallback() {
            @Override
            public void onSuccess(BatchReceiveResult receiveResult) {
                List<Message> messages = receiveResult.getMessageList();
                List<Long> handler = new ArrayList<>();
                for (Message message : messages) {
                    Assert.assertEquals(msg, message.getData());
                    handler.add(message.getReceiptHandle());
                    atomicInteger.incrementAndGet();
                }
                consumer.batchDeleteMsg(queueName, handler);
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                throw new RuntimeException(e);
            }
        });
        countDownLatch.await(20, TimeUnit.SECONDS);
        Assert.assertEquals(3, atomicInteger.get());

        CountDownLatch countDownLatch2 = new CountDownLatch(1);
        final AtomicInteger atomicInteger2 = new AtomicInteger(0);
        long start = System.currentTimeMillis();
        consumer.batchReceiveMsg(queueName, 10, 5, new BatchReceiveCallback() {
            @Override
            public void onSuccess(BatchReceiveResult receiveResult) {
                List<Message> messages = receiveResult.getMessageList();
                List<Long> handler = new ArrayList<>();
                for (Message message : messages) {
                    Assert.assertEquals(msg, message.getData());
                    handler.add(message.getReceiptHandle());
                    atomicInteger2.incrementAndGet();
                }
                consumer.batchDeleteMsg(queueName, handler);
                countDownLatch2.countDown();
            }

            @Override
            public void onException(Throwable e) {
                throw new RuntimeException(e);
            }
        });
        countDownLatch2.await(20, TimeUnit.SECONDS);
        long cost = System.currentTimeMillis() - start;
        Assert.assertTrue(cost > 4000);
        Assert.assertEquals(0, atomicInteger2.get());
    }

    @Test
    public void testSubscribe() throws InterruptedException {
        int times = 10;
        long start = System.currentTimeMillis();
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(times);
        consumer.subscribe(queueName, new MessageListener() {
            @Override
            public List<Long> consumeMessage(String queue, List<Message> messageList) {
                if (messageList != null) {
                    atomicInteger.incrementAndGet();
                    for (Message message : messageList) {
                        consumer.deleteMsg(queue, message.getReceiptHandle());
                        countDownLatch.countDown();
                    }
                }
                Assert.assertEquals(queueName, queue);
                return null;
            }
        });

        for (int i = 0; i < times; i++) {
            producer.send(queueName, msg);
            Thread.sleep(500);
        }
        countDownLatch.await(20, TimeUnit.SECONDS);
        long cost = System.currentTimeMillis() - start;
        consumer.unSubscribe(queueName);
        Assert.assertTrue(cost > 500 * times);
        Assert.assertEquals(times, atomicInteger.get());

        System.out.println("total cost:" + cost + "  total receive:" + atomicInteger.get());
    }

    @Test
    public void deleteMsgCallback() throws InterruptedException {
        producer.send(queueName, msg);
        ReceiveResult receiveResult = consumer.receiveMsg(queueName);
        AtomicInteger atomicInteger = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        consumer.deleteMsg(queueName, receiveResult.getMessage().getReceiptHandle(), new DeleteCallback() {
            @Override
            public void onSuccess(DeleteResult deleteResult) {
                atomicInteger.set(1);
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                atomicInteger.set(2);
            }
        });
        countDownLatch.await(20, TimeUnit.SECONDS);
        Assert.assertEquals(1, atomicInteger.get());
    }

    @Test
    public void MultiThread() throws InterruptedException {
        int senderNum = 200;
        int messageNum = 500;
        AtomicInteger atomicInteger = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(senderNum);

        Thread[] senders = new Thread[senderNum];
        for (int i = 0; i < senderNum; i++) {
            senders[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < messageNum; j++) {
                        try {
                            producer.send(queueName, msg);
                        } catch (Exception e) {
                            atomicInteger.incrementAndGet();
                            e.printStackTrace();
                        }
                    }
                    countDownLatch.countDown();
                }
            });
            senders[i].start();
        }
        countDownLatch.await(1000, TimeUnit.SECONDS);
        Assert.assertEquals(atomicInteger.get(), 0);

        CountDownLatch receiverCountDownLatch =  new CountDownLatch(senderNum);
        Thread[] receiver = new Thread[senderNum];
        for (int i = 0; i < senderNum; i++) {
            receiver[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < messageNum; j++) {
                        try {
                            ReceiveResult receiveResult = consumer.receiveMsg(queueName);
                            consumer.deleteMsg(queueName, receiveResult.getMessage().getReceiptHandle());
                        } catch (Exception e) {
                            atomicInteger.incrementAndGet();
                            e.printStackTrace();
                        }
                    }
                    receiverCountDownLatch.countDown();
                }
            });
            receiver[i].start();
        }
        receiverCountDownLatch.await(1000,TimeUnit.SECONDS);
        Assert.assertEquals(atomicInteger.get(), 0);
    }
}
