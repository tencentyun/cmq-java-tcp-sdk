package com.qcloud.cmq.client.client;

import com.qcloud.cmq.client.consumer.*;
import com.qcloud.cmq.client.netty.CommunicationMode;
import com.qcloud.cmq.client.netty.RemoteException;
import com.qcloud.cmq.client.producer.*;
import com.qcloud.cmq.client.protocol.Cmq;

import java.util.List;

/**
 * @ClassName CMQClientInterceptor
 * @Description cmq client interceptor
 * @Author Connli
 * @Date 2021/1/1 上午10:18
 * @Version 1.0
 **/
public interface CMQClientInterceptor {

    SendResult sendIntercept(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                             final CommunicationMode communicationMode, final SendCallback sendCallback,
                             final int retryTimesWhenSendFailed, final ProducerImpl producer, Chain chain)
            throws InterruptedException, RemoteException;
    BatchSendResult batchSendIntercept(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                     final CommunicationMode communicationMode, final BatchSendCallback sendCallback,
                                     final int retryTimesWhenSendFailed, ProducerImpl producer, Chain chain)
            throws RemoteException, InterruptedException;
    ReceiveResult receiveIntercept(final List<String> accessList, final Cmq.CMQProto request,
                                   final long timeoutMillis, final CommunicationMode communicationMode,
                                   final ReceiveCallback callback, Chain chain)
            throws RemoteException, InterruptedException;
    BatchReceiveResult batchReceiveIntercept(final List<String> accessList, final Cmq.CMQProto request,
                                             final long timeoutMillis, final CommunicationMode communicationMode,
                                             final BatchReceiveCallback callback, Chain chain)
            throws RemoteException, InterruptedException;
    DeleteResult deleteIntercept(final List<String> accessList, final Cmq.CMQProto request,
                               final long timeoutMillis, final CommunicationMode communicationMode,
                               final DeleteCallback callback, Chain chain)
            throws RemoteException, InterruptedException;
    BatchDeleteResult batchDeleteIntercept(final List<String> accessList, final Cmq.CMQProto request,
                                         final long timeoutMillis, final CommunicationMode communicationMode,
                                         final BatchDeleteCallback callback, Chain chain)
            throws RemoteException, InterruptedException;
    PublishResult publishIntercept(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                   final CommunicationMode communicationMode, final PublishCallback publishCallback,
                                   final int retryTimesWhenSendFailed, ProducerImpl producer, Chain chain)
            throws RemoteException, InterruptedException;
    BatchPublishResult batchPublishIntercept(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                           final CommunicationMode communicationMode, final BatchPublishCallback publishCallback,
                                           final int retryTimesWhenSendFailed, ProducerImpl producer, Chain chain)
            throws RemoteException, InterruptedException;

    interface Chain {

        void start();

        void shutdown();

        SendResult sendMessage(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                               final CommunicationMode communicationMode, final SendCallback sendCallback,
                               final int retryTimesWhenSendFailed, final ProducerImpl producer)
                throws InterruptedException, RemoteException;

        BatchSendResult batchSendMessage(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                         final CommunicationMode communicationMode, final BatchSendCallback sendCallback,
                                         final int retryTimesWhenSendFailed, ProducerImpl producer)
                throws RemoteException, InterruptedException;

        ReceiveResult receiveMessage(final List<String> accessList, final Cmq.CMQProto request,
                                     final long timeoutMillis, final CommunicationMode communicationMode,
                                     final ReceiveCallback callback) throws RemoteException, InterruptedException;

        BatchReceiveResult batchReceiveMessage(final List<String> accessList, final Cmq.CMQProto request,
                                               final long timeoutMillis, final CommunicationMode communicationMode,
                                               final BatchReceiveCallback callback)
                throws RemoteException, InterruptedException;

        DeleteResult deleteMessage(final List<String> accessList, final Cmq.CMQProto request,
                                   final long timeoutMillis, final CommunicationMode communicationMode,
                                   final DeleteCallback callback)
                throws RemoteException, InterruptedException;

        BatchDeleteResult batchDeleteMessage(final List<String> accessList, final Cmq.CMQProto request,
                                             final long timeoutMillis, final CommunicationMode communicationMode,
                                             final BatchDeleteCallback callback)
                throws RemoteException, InterruptedException;

        PublishResult publishMessage(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                     final CommunicationMode communicationMode, final PublishCallback publishCallback,
                                     final int retryTimesWhenSendFailed, ProducerImpl producer)
                throws RemoteException, InterruptedException;

        BatchPublishResult batchPublishMessage(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                               final CommunicationMode communicationMode, final BatchPublishCallback publishCallback,
                                               final int retryTimesWhenSendFailed, ProducerImpl producer)
                throws RemoteException, InterruptedException;
    }

    class Chains implements Chain {
        private final CMQClient cmqClient;
        private final List<CMQClientInterceptor> interceptorList;

        public Chains(CMQClient cmqClient, List<CMQClientInterceptor> interceptorList) {
            this.cmqClient = cmqClient;
            this.interceptorList = interceptorList;
        }

        @Override
        public void start() {
            this.cmqClient.start();
        }

        @Override
        public void shutdown() {
            this.cmqClient.shutdown();
        }

        @Override
        public SendResult sendMessage(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                      final CommunicationMode communicationMode, final SendCallback sendCallback,
                                      final int retryTimesWhenSendFailed, final ProducerImpl producer)
                throws InterruptedException, RemoteException {
            return new DefaultChain(cmqClient, interceptorList).sendMessage(accessList, request, timeoutMillis,
                    communicationMode, sendCallback, retryTimesWhenSendFailed, producer);
        }

        @Override
        public BatchSendResult batchSendMessage(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                                final CommunicationMode communicationMode, final BatchSendCallback sendCallback,
                                                final int retryTimesWhenSendFailed, ProducerImpl producer)
                throws RemoteException, InterruptedException {
            return new DefaultChain(cmqClient, interceptorList).batchSendMessage(accessList, request, timeoutMillis,
                    communicationMode, sendCallback, retryTimesWhenSendFailed, producer);
        }

        @Override
        public ReceiveResult receiveMessage(final List<String> accessList, final Cmq.CMQProto request,
                                            final long timeoutMillis, final CommunicationMode communicationMode,
                                            final ReceiveCallback pullCallback)
                throws RemoteException, InterruptedException {
            return new DefaultChain(cmqClient, interceptorList).receiveMessage(accessList, request, timeoutMillis,
                    communicationMode, pullCallback);
        }

        @Override
        public BatchReceiveResult batchReceiveMessage(final List<String> accessList, final Cmq.CMQProto request,
                                                      final long timeoutMillis, final CommunicationMode communicationMode,
                                                      final BatchReceiveCallback pullCallback)
                throws RemoteException, InterruptedException {
            return new DefaultChain(cmqClient, interceptorList).batchReceiveMessage(accessList, request, timeoutMillis,
                    communicationMode, pullCallback);
        }

        @Override
        public DeleteResult deleteMessage(final List<String> accessList, final Cmq.CMQProto request,
                                          final long timeoutMillis, final CommunicationMode communicationMode,
                                          final DeleteCallback callback)
                throws RemoteException, InterruptedException {
            return new DefaultChain(cmqClient, interceptorList).deleteMessage(accessList, request, timeoutMillis,
                    communicationMode, callback);
        }

        @Override
        public BatchDeleteResult batchDeleteMessage(final List<String> accessList, final Cmq.CMQProto request,
                                                    final long timeoutMillis, final CommunicationMode communicationMode,
                                                    final BatchDeleteCallback callback)
                throws RemoteException, InterruptedException {
            return new DefaultChain(cmqClient, interceptorList).batchDeleteMessage(accessList, request, timeoutMillis,
                    communicationMode, callback);
        }

        @Override
        public PublishResult publishMessage(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                            final CommunicationMode communicationMode, final PublishCallback publishCallback,
                                            final int retryTimesWhenSendFailed, ProducerImpl producer)
                throws RemoteException, InterruptedException {
            return new DefaultChain(cmqClient, interceptorList).publishMessage(accessList, request, timeoutMillis,
                    communicationMode, publishCallback, retryTimesWhenSendFailed, producer);
        }

        @Override
        public BatchPublishResult batchPublishMessage(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                                      final CommunicationMode communicationMode, final BatchPublishCallback publishCallback,
                                                      final int retryTimesWhenSendFailed, ProducerImpl producer)
                throws RemoteException, InterruptedException {
            return new DefaultChain(cmqClient, interceptorList).batchPublishMessage(accessList, request, timeoutMillis,
                    communicationMode, publishCallback, retryTimesWhenSendFailed, producer);
        }
    }

    class DefaultChain implements Chain {

        private CMQClient cmqClient;

        private int index = 0;

        private List<CMQClientInterceptor> interceptorList;

        public DefaultChain(CMQClient cmqClient,List<CMQClientInterceptor> interceptorList ) {
            this.cmqClient = cmqClient;
            this.interceptorList = interceptorList;
        }

        @Override
        public void start() {
            this.cmqClient.start();
        }

        @Override
        public void shutdown() {
            this.cmqClient.shutdown();
        }

        @Override
        public SendResult sendMessage(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                               final CommunicationMode communicationMode, final SendCallback sendCallback,
                               final int retryTimesWhenSendFailed, final ProducerImpl producer)
                throws InterruptedException, RemoteException {
            if (index == interceptorList.size()) {
                return cmqClient.sendMessage(accessList, request, timeoutMillis,
                        communicationMode, sendCallback, retryTimesWhenSendFailed, producer);
            } else {
                return interceptorList.get(index++).sendIntercept(accessList, request, timeoutMillis,
                        communicationMode, sendCallback, retryTimesWhenSendFailed, producer, this); // 传this(调度器)用于回调
            }
        }

        @Override
        public BatchSendResult batchSendMessage(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                                final CommunicationMode communicationMode, final BatchSendCallback sendCallback,
                                                final int retryTimesWhenSendFailed, ProducerImpl producer)
                throws RemoteException, InterruptedException {
            if (index == interceptorList.size()) {
                return cmqClient.batchSendMessage(accessList, request, timeoutMillis,
                        communicationMode, sendCallback, retryTimesWhenSendFailed, producer);
            } else {
                return interceptorList.get(index++).batchSendIntercept(accessList, request, timeoutMillis,
                        communicationMode, sendCallback, retryTimesWhenSendFailed, producer, this); // 传this(调度器)用于回调
            }
        }

        @Override
        public ReceiveResult receiveMessage(final List<String> accessList, final Cmq.CMQProto request,
                                            final long timeoutMillis, final CommunicationMode communicationMode,
                                            final ReceiveCallback pullCallback)
                throws RemoteException, InterruptedException {
            if (index == interceptorList.size()) {
                return cmqClient.receiveMessage(accessList, request, timeoutMillis,
                        communicationMode, pullCallback);
            } else {
                return interceptorList.get(index++).receiveIntercept(accessList, request, timeoutMillis,
                        communicationMode, pullCallback, this); // 传this(调度器)用于回调
            }
        }

        @Override
        public BatchReceiveResult batchReceiveMessage(final List<String> accessList, final Cmq.CMQProto request,
                                                      final long timeoutMillis, final CommunicationMode communicationMode,
                                                      final BatchReceiveCallback pullCallback)
                throws RemoteException, InterruptedException  {
            if (index == interceptorList.size()) {
                return cmqClient.batchReceiveMessage(accessList, request, timeoutMillis, communicationMode, pullCallback);
            } else {
                return interceptorList.get(index++).batchReceiveIntercept(accessList, request, timeoutMillis,
                        communicationMode, pullCallback, this); // 传this(调度器)用于回调
            }
        }

        @Override
        public DeleteResult deleteMessage(final List<String> accessList, final Cmq.CMQProto request,
                                                      final long timeoutMillis, final CommunicationMode communicationMode,
                                                      final DeleteCallback pullCallback)
                throws RemoteException, InterruptedException  {
            if (index == interceptorList.size()) {
                return cmqClient.deleteMessage(accessList, request, timeoutMillis, communicationMode, pullCallback);
            } else {
                return interceptorList.get(index++).deleteIntercept(accessList, request, timeoutMillis,
                        communicationMode, pullCallback, this); // 传this(调度器)用于回调
            }
        }

        @Override
        public BatchDeleteResult batchDeleteMessage(final List<String> accessList, final Cmq.CMQProto request,
                                                    final long timeoutMillis, final CommunicationMode communicationMode,
                                                    final BatchDeleteCallback batchDeleteCallback)
                throws RemoteException, InterruptedException {
            if (index == interceptorList.size()) {
                return cmqClient.batchDeleteMessage(accessList, request, timeoutMillis, communicationMode, batchDeleteCallback);
            } else {
                return interceptorList.get(index++).batchDeleteIntercept(accessList, request, timeoutMillis,
                        communicationMode, batchDeleteCallback, this); // 传this(调度器)用于回调
            }
        }

        @Override
        public PublishResult publishMessage(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                            final CommunicationMode communicationMode, final PublishCallback publishCallback,
                                            final int retryTimesWhenSendFailed, ProducerImpl producer)
                throws RemoteException, InterruptedException {
            if (index == interceptorList.size()) {
                return cmqClient.publishMessage(accessList, request, timeoutMillis,
                        communicationMode, publishCallback, retryTimesWhenSendFailed, producer);
            } else {
                return interceptorList.get(index++).publishIntercept(accessList, request, timeoutMillis,
                        communicationMode, publishCallback, retryTimesWhenSendFailed, producer, this); // 传this(调度器)用于回调
            }
        }

        @Override
        public BatchPublishResult batchPublishMessage(List<String> accessList, final Cmq.CMQProto request, final long timeoutMillis,
                                            final CommunicationMode communicationMode, final BatchPublishCallback publishCallback,
                                            final int retryTimesWhenSendFailed, ProducerImpl producer)
                throws RemoteException, InterruptedException {
            if (index == interceptorList.size()) {
                return cmqClient.batchPublishMessage(accessList, request, timeoutMillis,
                        communicationMode, publishCallback, retryTimesWhenSendFailed, producer);
            } else {
                return interceptorList.get(index++).batchPublishIntercept(accessList, request, timeoutMillis,
                        communicationMode, publishCallback, retryTimesWhenSendFailed, producer, this); // 传this(调度器)用于回调
            }
        }
    }
}

