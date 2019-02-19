package com.qcloud.cmq.client.client;

import com.google.protobuf.TextFormat;
import com.qcloud.cmq.client.consumer.*;
import com.qcloud.cmq.client.producer.*;
import com.qcloud.cmq.client.common.*;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;
import com.qcloud.cmq.client.netty.*;
import com.qcloud.cmq.client.protocol.Cmq;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class CMQClient {

    private final static Logger logger = LogHelper.getLog();
    private ClientConfig clientConfig;
    private String clientId;
    private final NettyClient nettyClient;

    public CMQClient(final NettyClientConfig nettyClientConfig, final CMQClientHandler cMQClientHandler,
                     final ClientConfig clientConfig, final String clientId) {
        this.clientConfig = clientConfig;
        this.clientId = clientId;
        this.nettyClient = new NettyClient(nettyClientConfig, cMQClientHandler);
    }

    public void start() {
        this.nettyClient.setAuthData(this.createAuthData());
        this.nettyClient.start();
    }

    public void shutdown() {
        this.nettyClient.shutdown();
    }

    private Cmq.cmq_tcp_auth createAuthData() {
        String signature;
        try {
            signature = SignTool.sign(this.clientId + this.clientConfig.getSecretId(),
                    this.clientConfig.getSecretKey(), this.clientConfig.getSignMethod());
        } catch (Exception e) {
            logger.error("create auth data error.", e);
            return null;
        }
        return Cmq.cmq_tcp_auth.newBuilder()
                .setClientId(this.clientId)
                .setSecretId(this.clientConfig.getSecretId())
                .setSignatureMethod(this.clientConfig.getSignMethod())
                .setSignature(signature).build();
    }

    public SendResult sendMessage(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                  final CommunicationMode communicationMode, final SendCallback sendCallback,
                                  final int retryTimesWhenSendFailed, final ProducerImpl producer)
            throws InterruptedException, RemoteException {
        switch (communicationMode) {
            case SYNC:
                return this.sendMessageSync(accessList, request, timeoutMillis);
            case ASYNC:
                final AtomicInteger times = new AtomicInteger();
                this.sendMessageAsync(accessList, timeoutMillis, request, sendCallback, retryTimesWhenSendFailed, times, producer);
                return null;
            case ONEWAY:
                this.nettyClient.invokeOneWay(accessList, request, timeoutMillis);
                return null;
            default:
                assert false;
                break;
        }
        return null;
    }

    private SendResult sendMessageSync(final List<String> addressList, final Cmq.CMQProto request, final long timeoutMillis)
            throws InterruptedException, RemoteException {
        Cmq.CMQProto response = this.nettyClient.invokeSync(addressList, request, timeoutMillis);
        assert response != null;
        return this.processSendResponse(response);
    }

    private void sendMessageAsync(final List<String> accessList, final long timeoutMillis, final Cmq.CMQProto request,
                                  final SendCallback sendCallback, final int retryTimesWhenSendFailed,
                                  final AtomicInteger times, final ProducerImpl producer)
            throws InterruptedException, RemoteException {
        this.nettyClient.invokeAsync(accessList, request, timeoutMillis, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {
                Cmq.CMQProto response = responseFuture.getResponseCommand();
                if (response != null) {
                    SendResult sendResult = CMQClient.this.processSendResponse(response);
                    if (sendCallback != null) {
                        try {
                            sendCallback.onSuccess(sendResult);
                        } catch (Throwable e) {
                            // ignore
                        }
                    }
                } else {
                    MQClientException ex = new MQClientException(responseFuture.getErrorMsg("sendMessageAsync"), responseFuture.getCause());
                    onExceptionImpl(accessList, timeoutMillis, request, sendCallback, retryTimesWhenSendFailed, times, ex , true, producer);
                }
            }
        });
    }

    private void onExceptionImpl(List<String> accessList, final long timeoutMillis, final Cmq.CMQProto request,
                                 final SendCallback sendCallback, final int timesTotal, final AtomicInteger curTimes,
                                 final Exception e, final boolean needRetry, ProducerImpl producer) {
        int tmp = curTimes.incrementAndGet();
        if (needRetry && tmp <= timesTotal) {
            logger.info("async send msg by retry {} times", tmp);
            Cmq.CMQProto newRequest = Cmq.CMQProto.newBuilder(request).setSeqno(RequestIdHelper.getNextSeqNo()).build();
            try {
                if (tmp == 1) {
                    accessList = producer.findQueueRoute(request.getTcpSendMsg().getQueueName(), true);
                }
                sendMessageAsync(accessList, timeoutMillis, newRequest, sendCallback, timesTotal, curTimes, producer);
            } catch (InterruptedException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, sendCallback, timesTotal, curTimes, e1, false, producer);
            } catch (RemoteTooMuchRequestException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, sendCallback, timesTotal, curTimes, e1, false, producer);
            } catch (MQClientException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, sendCallback, timesTotal, curTimes, e1, false, producer);
            } catch (MQServerException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, sendCallback, timesTotal, curTimes, e1, true, producer);
            } catch (RemoteException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, sendCallback, timesTotal, curTimes, e1, true, producer);
            }
        } else {
            try {
                sendCallback.onException(e);
            } catch (Exception ignored) {
                // ignore
            }
        }
    }

    private SendResult processSendResponse(final Cmq.CMQProto response) {
        long msgId = -1;
        if (response.getMsgidsCount() > 0) {
            msgId = response.getMsgids(0);
        }
        return new SendResult(response.getResult(), msgId, response.getSeqno(), response.getError());
    }

    public BatchSendResult batchSendMessage(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                            final CommunicationMode communicationMode, final BatchSendCallback sendCallback,
                                            final int retryTimesWhenSendFailed, ProducerImpl producer)
            throws RemoteException, InterruptedException {
        switch (communicationMode) {
            case SYNC:
                return this.batchSendMessageSync(accessList, request, timeoutMillis);
            case ASYNC:
                final AtomicInteger times = new AtomicInteger();
                this.batchSendMessageAsync(accessList, timeoutMillis, request, sendCallback, retryTimesWhenSendFailed, times, producer);
                return null;
            case ONEWAY:
                this.nettyClient.invokeOneWay(accessList, request, timeoutMillis);
                return null;
            default:
                assert false;
                break;
        }
        return null;
    }

    private BatchSendResult batchSendMessageSync(final List<String> addressList, final Cmq.CMQProto request, final long timeoutMillis)
            throws RemoteException, InterruptedException {
        Cmq.CMQProto response = this.nettyClient.invokeSync(addressList, request, timeoutMillis);
        assert response != null;
        return new BatchSendResult(response.getResult(), response.getSeqno(), response.getError(), response.getMsgidsList());
    }

    private void batchSendMessageAsync(final List<String> accessList, final long timeoutMillis, final Cmq.CMQProto request,
                                       final BatchSendCallback batchSendCallback, final int retryTimesWhenSendFailed,
                                       final AtomicInteger times, final ProducerImpl producer)
            throws InterruptedException, RemoteException {
        this.nettyClient.invokeAsync(accessList, request, timeoutMillis, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {
                Cmq.CMQProto response = responseFuture.getResponseCommand();
                if (response != null) {
                    BatchSendResult sendResult = new BatchSendResult(response.getResult(),
                            response.getSeqno(), response.getError(), response.getMsgidsList());
                    if (batchSendCallback != null) {
                        try {
                            batchSendCallback.onSuccess(sendResult);
                        } catch (Throwable e) {
                            // ignore
                        }
                    }
                } else {
                    MQClientException ex = new MQClientException(responseFuture.getErrorMsg("batchSendMessageAsync"), responseFuture.getCause());
                    onExceptionImpl(accessList, timeoutMillis, request, batchSendCallback, retryTimesWhenSendFailed, times, ex, true, producer);
                }
            }
        });
    }

    private void onExceptionImpl(List<String> accessList, final long timeoutMillis, final Cmq.CMQProto request,
                                 final BatchSendCallback sendCallback, final int timesTotal, final AtomicInteger curTimes,
                                 final Exception e, final boolean needRetry, ProducerImpl producer) {
        int tmp = curTimes.incrementAndGet();
        if (needRetry && tmp <= timesTotal) {
            logger.info("async send msg by retry {} times", tmp);
            Cmq.CMQProto newRequest = Cmq.CMQProto.newBuilder(request).setSeqno(RequestIdHelper.getNextSeqNo()).build();
            try {
                if (tmp == 1) {
                    accessList = producer.findQueueRoute(request.getTcpBatchSendMsg().getQueueName(), true);
                }
                batchSendMessageAsync(accessList, timeoutMillis, newRequest, sendCallback, timesTotal, curTimes, producer);
            } catch (MQClientException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, sendCallback, timesTotal, curTimes, e1, false, producer);
            } catch (InterruptedException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, sendCallback, timesTotal, curTimes, e1, false, producer);
            } catch (RemoteTooMuchRequestException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, sendCallback, timesTotal, curTimes, e1, false, producer);
            } catch (MQServerException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, sendCallback, timesTotal, curTimes, e1, true, producer);
            } catch (RemoteException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, sendCallback, timesTotal, curTimes, e1, true, producer);
            }
        } else {
            try {
                sendCallback.onException(e);
            } catch (Exception ignored) {
            }
        }
    }

    public PublishResult publishMessage(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                        final CommunicationMode communicationMode, final PublishCallback publishCallback,
                                        final int retryTimesWhenSendFailed, ProducerImpl producer)
            throws RemoteException, InterruptedException {
        switch (communicationMode) {
            case SYNC:
                return this.publishMessageSync(accessList, request, timeoutMillis);
            case ASYNC:
                final AtomicInteger times = new AtomicInteger();
                this.publishMessageAsync(accessList, timeoutMillis, request, publishCallback, retryTimesWhenSendFailed, times, producer);
                return null;
            case ONEWAY:
                this.nettyClient.invokeOneWay(accessList, request, timeoutMillis);
                return null;
            default:
                assert false;
                break;
        }
        return null;
    }

    private PublishResult publishMessageSync(final List<String> addressList, final Cmq.CMQProto request, final long timeoutMillis)
            throws RemoteException, InterruptedException {
        Cmq.CMQProto response = this.nettyClient.invokeSync(addressList, request, timeoutMillis);
        assert response != null;
        return this.processPublishResponse(response);
    }

    private void publishMessageAsync(final List<String> accessList, final long timeoutMillis, final Cmq.CMQProto request,
                                     final PublishCallback publishCallback, final int retryTimesWhenSendFailed,
                                     final AtomicInteger times, final ProducerImpl producer)
            throws InterruptedException, RemoteException {
        this.nettyClient.invokeAsync(accessList, request, timeoutMillis, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {
                Cmq.CMQProto response = responseFuture.getResponseCommand();
                if (response != null) {
                    PublishResult publishResult = CMQClient.this.processPublishResponse(response);
                    if (publishCallback != null) {
                        try {
                            publishCallback.onSuccess(publishResult);
                        } catch (Throwable e) {
                            // ignore
                        }
                    }
                } else {
                    MQClientException ex = new MQClientException(responseFuture.getErrorMsg("publishMessageAsync"), responseFuture.getCause());
                    onExceptionImpl(accessList, timeoutMillis, request, publishCallback, retryTimesWhenSendFailed, times, ex, true, producer);
                }
            }
        });
    }

    private void onExceptionImpl(List<String> accessList, final long timeoutMillis, final Cmq.CMQProto request,
                                 final PublishCallback publishCallback, final int timesTotal, final AtomicInteger curTimes,
                                 final Exception e, final boolean needRetry, ProducerImpl producer) {
        int tmp = curTimes.incrementAndGet();
        if (needRetry && tmp <= timesTotal) {
            logger.info("async send msg by retry {} times", tmp);
            Cmq.CMQProto newRequest = Cmq.CMQProto.newBuilder(request).setSeqno(RequestIdHelper.getNextSeqNo()).build();
            try {
                if (tmp == 1) {
                    accessList = producer.findTopicRoute(request.getTcpPublishMsg().getTopicName(), true);
                }
                publishMessageAsync(accessList, timeoutMillis, newRequest, publishCallback, timesTotal, curTimes, producer);
            } catch (MQClientException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, publishCallback, timesTotal, curTimes, e1, false, producer);
            } catch (InterruptedException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, publishCallback, timesTotal, curTimes, e1, false, producer);
            } catch (RemoteTooMuchRequestException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, publishCallback, timesTotal, curTimes, e1, false, producer);
            } catch (MQServerException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, publishCallback, timesTotal, curTimes, e1, true, producer);
            } catch (RemoteException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, publishCallback, timesTotal, curTimes, e1, true, producer);
            }
        } else {
            try {
                publishCallback.onException(e);
            } catch (Exception ignored) {
            }
        }
    }

    private PublishResult processPublishResponse(final Cmq.CMQProto response) {
        long msgId = -1;
        if (response.getMsgidsCount() > 0) {
            msgId = response.getMsgids(0);
        }
        return new PublishResult(response.getResult(), msgId, response.getSeqno(), response.getError());
    }

    public BatchPublishResult batchPublishMessage(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                                  final CommunicationMode communicationMode, final BatchPublishCallback publishCallback,
                                                  final int retryTimesWhenSendFailed, ProducerImpl producer)
            throws RemoteException, InterruptedException {
        switch (communicationMode) {
            case SYNC:
                return this.batchPublishMessageSync(accessList, request, timeoutMillis);
            case ASYNC:
                final AtomicInteger times = new AtomicInteger();
                this.batchPublishMessageAsync(accessList, timeoutMillis, request, publishCallback, retryTimesWhenSendFailed, times, producer);
                return null;
            case ONEWAY:
                this.nettyClient.invokeOneWay(accessList, request, timeoutMillis);
                return null;
            default:
                assert false;
                break;
        }
        return null;
    }

    private BatchPublishResult batchPublishMessageSync(final List<String> addressList, final Cmq.CMQProto request, final long timeoutMillis)
            throws RemoteException, InterruptedException {
        Cmq.CMQProto response = this.nettyClient.invokeSync(addressList, request, timeoutMillis);
        assert response != null;
        return new BatchPublishResult(response.getResult(), response.getSeqno(), response.getError(), response.getMsgidsList());
    }

    private void batchPublishMessageAsync(final List<String> accessList, final long timeoutMillis, final Cmq.CMQProto request,
                                          final BatchPublishCallback publishCallback, final int retryTimesWhenSendFailed,
                                          final AtomicInteger times, final ProducerImpl producer)
            throws InterruptedException, RemoteException {
        this.nettyClient.invokeAsync(accessList, request, timeoutMillis, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {
                Cmq.CMQProto response = responseFuture.getResponseCommand();
                if (response != null) {
                    BatchPublishResult sendResult = new BatchPublishResult(response.getResult(),
                            response.getSeqno(), response.getError(), response.getMsgidsList());
                    if (publishCallback != null) {
                        try {
                            publishCallback.onSuccess(sendResult);
                        } catch (Throwable e) {
                            // ignore
                        }
                    }
                } else {
                    MQClientException ex = new MQClientException(responseFuture.getErrorMsg("batchPublishMessageAsync"), responseFuture.getCause());
                    onExceptionImpl(accessList, timeoutMillis, request, publishCallback, retryTimesWhenSendFailed, times, ex, true, producer);
                }
            }
        });
    }

    private void onExceptionImpl(List<String> accessList, final long timeoutMillis, final Cmq.CMQProto request,
                                 final BatchPublishCallback publishCallback, final int timesTotal, final AtomicInteger curTimes,
                                 final Exception e, final boolean needRetry, ProducerImpl producer) {
        int tmp = curTimes.incrementAndGet();
        if (needRetry && tmp <= timesTotal) {
            logger.info("async send msg by retry {} times", tmp);
            Cmq.CMQProto newRequest = Cmq.CMQProto.newBuilder(request).setSeqno(RequestIdHelper.getNextSeqNo()).build();
            try {
                if (tmp == 1) {
                    accessList = producer.findTopicRoute(request.getTcpBatchPublishMsg().getTopicName(), true);
                }
                batchPublishMessageAsync(accessList, timeoutMillis, newRequest, publishCallback, timesTotal, curTimes, producer);
            } catch (MQClientException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, publishCallback, timesTotal, curTimes, e1, false, producer);
            } catch (InterruptedException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, publishCallback, timesTotal, curTimes, e1, false, producer);
            } catch (RemoteTooMuchRequestException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, publishCallback, timesTotal, curTimes, e1, false, producer);
            } catch (MQServerException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, publishCallback, timesTotal, curTimes, e1, true, producer);
            } catch (RemoteException e1) {
                onExceptionImpl(accessList, timeoutMillis, newRequest, publishCallback, timesTotal, curTimes, e1, true, producer);
            }
        } else {
            try {
                publishCallback.onException(e);
            } catch (Exception ignored) {
            }
        }
    }

    public ReceiveResult receiveMessage(final List<String> accessList, final Cmq.CMQProto request,
                                        final long timeoutMillis, final CommunicationMode communicationMode,
                                        final ReceiveCallback pullCallback)
            throws RemoteException, InterruptedException {
        switch (communicationMode) {
            case ONEWAY:
                assert false;
                return null;
            case ASYNC:
                this.receiveMessageAsync(accessList, request, timeoutMillis, pullCallback);
                return null;
            case SYNC:
                return this.receiveMessageSync(accessList, request, timeoutMillis);
            default:
                assert false;
                break;
        }
        return null;
    }

    private void receiveMessageAsync(final List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                     final ReceiveCallback pullCallback) throws RemoteException, InterruptedException {
        this.nettyClient.invokeAsync(accessList, request, timeoutMillis, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {
                Cmq.CMQProto response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        ReceiveResult receiveResult = CMQClient.this.processReceiveResponse(response);
                        pullCallback.onSuccess(receiveResult);
                    } catch (Exception e) {
                        pullCallback.onException(e);
                    }
                } else {
                    String prefix = String.format("receiveMessageAsync:Server[%s], request[%s]", accessList, TextFormat.shortDebugString(request));
                    pullCallback.onException(new MQClientException(responseFuture.getErrorMsg(prefix), responseFuture.getCause()));
                }
            }
        });
    }

    private ReceiveResult receiveMessageSync(final List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis)
            throws RemoteException, InterruptedException {
        Cmq.CMQProto response = this.nettyClient.invokeSync(accessList, request, timeoutMillis);
        assert response != null;
        return this.processReceiveResponse(response);
    }

    private ReceiveResult processReceiveResponse(final Cmq.CMQProto response) {
        Cmq.cmq_pull_msg_rsp responseContent = response.getPullRsp();
        Message message = new Message(responseContent.getMsgId(), responseContent.getReceiptHandle(), responseContent.getMsgBody().toStringUtf8());
        return new ReceiveResult(response.getResult(), response.getSeqno(), response.getError(), message);
    }

    public BatchReceiveResult batchReceiveMessage(final List<String> accessList, final Cmq.CMQProto request,
                                                  final long timeoutMillis, final CommunicationMode communicationMode,
                                                  final BatchReceiveCallback pullCallback)
            throws RemoteException, InterruptedException {
        switch (communicationMode) {
            case ONEWAY:
                assert false;
                return null;
            case ASYNC:
                this.batchReceiveMessageAsync(accessList, request, timeoutMillis, pullCallback);
                return null;
            case SYNC:
                return this.batchReceiveMessageSync(accessList, request, timeoutMillis);
            default:
                assert false;
                break;
        }
        return null;
    }

    private void batchReceiveMessageAsync(final List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                          final BatchReceiveCallback pullCallback) throws RemoteException, InterruptedException {
        this.nettyClient.invokeAsync(accessList, request, timeoutMillis, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {
                Cmq.CMQProto response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        BatchReceiveResult receiveResult = CMQClient.this.processBatchReceiveResponse(response);
                        pullCallback.onSuccess(receiveResult);
                    } catch (Exception e) {
                        pullCallback.onException(e);
                    }
                } else {
                    String prefix = String.format("batchReceiveMessageAsync:Server[%s], request[%s]", accessList, TextFormat.shortDebugString(request));
                    pullCallback.onException(new MQClientException(responseFuture.getErrorMsg(prefix), responseFuture.getCause()));
                }
            }
        });
    }

    private BatchReceiveResult batchReceiveMessageSync(final List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis)
            throws RemoteException, InterruptedException {
        Cmq.CMQProto response = this.nettyClient.invokeSync(accessList, request, timeoutMillis);
        assert response != null;
        return this.processBatchReceiveResponse(response);
    }

    private BatchReceiveResult processBatchReceiveResponse(final Cmq.CMQProto response) {
        Cmq.cmq_batch_pull_msg_rsp responseContent = response.getBatchPullRsp();
        List<Message> messageList = new ArrayList<Message>();
        for (Cmq.cmq_pull_msg_rsp rsp : responseContent.getMsgListList()) {
            messageList.add(new Message(rsp.getMsgId(), rsp.getReceiptHandle(), rsp.getMsgBody().toStringUtf8()));
        }
        return new BatchReceiveResult(response.getResult(), response.getSeqno(), response.getError(), messageList);
    }

    public DeleteResult deleteMessage(final List<String> accessList, final Cmq.CMQProto request,
                                      final long timeoutMillis, final CommunicationMode communicationMode,
                                      final DeleteCallback pullCallback)
            throws RemoteException, InterruptedException {
        switch (communicationMode) {
            case ONEWAY:
                assert false;
                return null;
            case ASYNC:
                this.deleteMessageAsync(accessList, request, timeoutMillis, pullCallback);
                return null;
            case SYNC:
                return this.deleteMessageSync(accessList, request, timeoutMillis);
            default:
                assert false;
                break;
        }
        return null;
    }

    private void deleteMessageAsync(final List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                    final DeleteCallback callback) throws RemoteException, InterruptedException {
        this.nettyClient.invokeAsync(accessList, request, timeoutMillis, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {
                Cmq.CMQProto response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        DeleteResult deleteResult = new DeleteResult(response.getResult(), response.getSeqno(), response.getError());
                        callback.onSuccess(deleteResult);
                    } catch (Exception e) {
                        callback.onException(e);
                    }
                } else {
                    String prefix = String.format("deleteMessageAsync:Server[%s], request[%s]", accessList, TextFormat.shortDebugString(request));
                    callback.onException(new MQClientException(responseFuture.getErrorMsg(prefix), responseFuture.getCause()));
                }
            }
        });
    }

    private DeleteResult deleteMessageSync(final List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis)
            throws RemoteException, InterruptedException {
        Cmq.CMQProto response = this.nettyClient.invokeSync(accessList, request, timeoutMillis);
        assert response != null;
        return new DeleteResult(response.getResult(), response.getSeqno(), response.getError());
    }


    public BatchDeleteResult batchDeleteMessage(final List<String> accessList, final Cmq.CMQProto request,
                                                  final long timeoutMillis, final CommunicationMode communicationMode,
                                                  final BatchDeleteCallback callback)
            throws RemoteException, InterruptedException {
        switch (communicationMode) {
            case ONEWAY:
                assert false;
                return null;
            case ASYNC:
                this.batchDeleteMessageAsync(accessList, request, timeoutMillis, callback);
                return null;
            case SYNC:
                return this.batchDeleteMessageSync(accessList, request, timeoutMillis);
            default:
                assert false;
                break;
        }
        return null;
    }

    private void batchDeleteMessageAsync(final List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                          final BatchDeleteCallback callback) throws RemoteException, InterruptedException {
        this.nettyClient.invokeAsync(accessList, request, timeoutMillis, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {
                Cmq.CMQProto response = responseFuture.getResponseCommand();
                if (response != null) {
                    try {
                        BatchDeleteResult deleteResult = CMQClient.this.processBatchDeleteResponse(response);
                        callback.onSuccess(deleteResult);
                    } catch (Exception e) {
                        callback.onException(e);
                    }
                } else {
                    String prefix = String.format("batchDeleteMessageAsync:Server[%s], request[%s]", accessList, TextFormat.shortDebugString(request));
                    callback.onException(new MQClientException(responseFuture.getErrorMsg(prefix), responseFuture.getCause()));
                }
            }
        });
    }

    private BatchDeleteResult batchDeleteMessageSync(final List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis)
            throws RemoteException, InterruptedException {
        Cmq.CMQProto response = this.nettyClient.invokeSync(accessList, request, timeoutMillis);
        assert response != null;
        return this.processBatchDeleteResponse(response);
    }

    private BatchDeleteResult processBatchDeleteResponse(final Cmq.CMQProto response) {
        List<Cmq.cmq_msg_delete_result> responseContent = response.getDelResultList();
        List<ReceiptHandleErrorInfo> errorInfoList = new ArrayList<ReceiptHandleErrorInfo>();
        for (Cmq.cmq_msg_delete_result rsp : responseContent) {
            errorInfoList.add(new ReceiptHandleErrorInfo(rsp.getErrCode(), rsp.getErrMsg(), rsp.getReceiptHandle()));
        }
        return new BatchDeleteResult(response.getResult(), response.getSeqno(), response.getError(), errorInfoList);
    }

}