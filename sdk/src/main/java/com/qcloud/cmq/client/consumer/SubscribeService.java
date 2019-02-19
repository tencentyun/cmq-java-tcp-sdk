package com.qcloud.cmq.client.consumer;

import com.qcloud.cmq.client.client.ThreadGroupFactory;
import com.qcloud.cmq.client.common.LogHelper;
import com.qcloud.cmq.client.common.RemoteHelper;
import com.qcloud.cmq.client.common.ResponseCode;
import com.qcloud.cmq.client.common.RequestIdHelper;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.netty.CommunicationMode;
import com.qcloud.cmq.client.netty.RemoteException;
import com.qcloud.cmq.client.protocol.Cmq;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SubscribeService {
    private final static Logger logger = LogHelper.getLog();

    private final String queue;
    private final MessageListener listener;
    private final Cmq.CMQProto.Builder pullRequestBuilder;
    private final ConsumerImpl consumer;

    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;
    private final ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "PullMessageScheduledThread");
                }
            });
    private AtomicInteger flightPullRequest = new AtomicInteger();

    SubscribeService(String queue, MessageListener listener, Cmq.CMQProto.Builder builder, ConsumerImpl consumer) {
        this.queue = queue;
        this.listener = listener;
        this.pullRequestBuilder = builder;
        this.consumer = consumer;
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();
        this.consumeExecutor = new ThreadPoolExecutor(1, 1, 1000 * 60, TimeUnit.MILLISECONDS,
                this.consumeRequestQueue, new ThreadGroupFactory("ConsumeMessageThread_"));
    }

    private void startScheduleTask() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                logger.debug("schedule flightPullRequest:{}, size:{}, active: {}",
                        flightPullRequest.get(), consumeRequestQueue.size(), consumeExecutor.getActiveCount());
                if (flightPullRequest.get() < 16 && consumeRequestQueue.size() < 16) {
                    flightPullRequest.incrementAndGet();
                    SubscribeService.this.pullImmediately();
                }
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    private void submitPullRequest() {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                logger.debug("submit flightPullRequest:{}, size:{}", flightPullRequest.get(), consumeRequestQueue.size());
                if (flightPullRequest.get() < 16 && consumeRequestQueue.size() < 16) {
                    flightPullRequest.incrementAndGet();
                    SubscribeService.this.pullImmediately();
                }
            }
        }, 0, TimeUnit.MILLISECONDS);
    }

    private void pullImmediately() {
        Cmq.CMQProto request = pullRequestBuilder.setSeqno(RequestIdHelper.getNextSeqNo()).build();
        int timeoutMS = this.consumer.getConsumer().getRequestTimeoutMS() + this.consumer.getConsumer().getPollingWaitSeconds() * 1000;

        try {
            List<String> accessList = consumer.getQueueRoute(this.queue);
            this.consumer.getMQClientInstance().getCMQClient().batchReceiveMessage(accessList, request, timeoutMS,
                    CommunicationMode.ASYNC, new BatchReceiveCallback() {
                        @Override
                        public void onSuccess(BatchReceiveResult receiveResult) {
                            flightPullRequest.decrementAndGet();
                            if (receiveResult.getReturnCode() == ResponseCode.SUCCESS) {
                                logger.debug("offer consume request");
                                consumeExecutor.submit(new ConsumeRequest(receiveResult.getMessageList()));
                                submitPullRequest();
                            }
                        }

                        @Override
                        public void onException(Throwable e) {
                            flightPullRequest.decrementAndGet();
                            submitPullRequest();
                        }
                    });
        } catch (RemoteException e) {
            logger.error("pull message error", e);
            this.consumer.setNeedUpdateRoute();
        } catch (InterruptedException e) {
            logger.error("pull message error", e);
            this.consumer.setNeedUpdateRoute();
        } catch (MQClientException e) {
            logger.error("pull message error", e);
            this.consumer.setNeedUpdateRoute();
        }
    }

    public void start() {
        this.startScheduleTask();
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.consumeExecutor.shutdown();
        logger.info("shutdown pull service for queue {} success.", queue);
    }

    class ConsumeRequest implements Runnable {

        private final List<Message> msgList;

        ConsumeRequest(List<Message> msgList) {
            this.msgList = msgList;
        }

        @Override
        public void run() {
            try {
                List<Long> ackMsg = listener.consumeMessage(queue, Collections.unmodifiableList(msgList));
                if (ackMsg != null && !ackMsg.isEmpty()) {
                    consumer.getConsumer().batchDeleteMsg(queue, ackMsg, new BatchDeleteCallback() {
                        @Override
                        public void onSuccess(BatchDeleteResult deleteResult) {
                            logger.debug("delete msg success, result{}", deleteResult);
                        }

                        @Override
                        public void onException(Throwable e) {
                            logger.debug("delete msg failed", e);
                        }
                    });
                }
                SubscribeService.this.submitPullRequest();
            } catch (Throwable e) {
                logger.warn("consumeMessage exception: {} queue: {}", RemoteHelper.exceptionSimpleDesc(e), queue);
            }
        }

    }
}
