package demo;

import com.qcloud.cmq.client.client.CMQClientInterceptor;
import com.qcloud.cmq.client.consumer.*;
import com.qcloud.cmq.client.netty.CommunicationMode;
import com.qcloud.cmq.client.netty.RemoteException;
import com.qcloud.cmq.client.producer.*;
import com.qcloud.cmq.client.protocol.Cmq;

import java.util.List;

public class Interceptor implements CMQClientInterceptor {

    @Override
    public SendResult sendIntercept(List<String> accessList, Cmq.CMQProto request, long timeoutMillis,
                                    CommunicationMode communicationMode, SendCallback sendCallback,
                                    int retryTimesWhenSendFailed, ProducerImpl producer, Chain chain)
            throws InterruptedException, RemoteException {
        System.out.println("sendIntercept");
        return chain.sendMessage(accessList, request, timeoutMillis, communicationMode, sendCallback,
                retryTimesWhenSendFailed, producer);
    }

    @Override
    public BatchSendResult batchSendIntercept(List<String> accessList, Cmq.CMQProto request, long timeoutMillis,
                                              CommunicationMode communicationMode, BatchSendCallback sendCallback,
                                              int retryTimesWhenSendFailed, ProducerImpl producer, Chain chain)
            throws RemoteException, InterruptedException {
        System.out.println("batchSendIntercept");
        return chain.batchSendMessage(accessList, request, timeoutMillis, communicationMode, sendCallback,
                retryTimesWhenSendFailed, producer);
    }

    @Override
    public ReceiveResult receiveIntercept(List<String> accessList, Cmq.CMQProto request, long timeoutMillis,
                                          CommunicationMode communicationMode, ReceiveCallback callback,
                                          Chain chain) throws RemoteException, InterruptedException {
        System.out.println("receiveIntercept");
        return chain.receiveMessage(accessList, request, timeoutMillis, communicationMode, callback);
    }

    @Override
    public BatchReceiveResult batchReceiveIntercept(List<String> accessList, Cmq.CMQProto request, long timeoutMillis,
                                                    CommunicationMode communicationMode, BatchReceiveCallback callback,
                                                    Chain chain) throws RemoteException, InterruptedException {
        System.out.println("batchReceiveIntercept");
        return chain.batchReceiveMessage(accessList, request, timeoutMillis, communicationMode, callback);
    }

    @Override
    public DeleteResult deleteIntercept(List<String> accessList, Cmq.CMQProto request, long timeoutMillis,
                                        CommunicationMode communicationMode, DeleteCallback callback,
                                        Chain chain) throws RemoteException, InterruptedException {
        System.out.println("deleteIntercept");
        return chain.deleteMessage(accessList, request, timeoutMillis, communicationMode, callback);
    }

    @Override
    public BatchDeleteResult batchDeleteIntercept(List<String> accessList, Cmq.CMQProto request, long timeoutMillis,
                                                  CommunicationMode communicationMode, BatchDeleteCallback callback,
                                                  Chain chain) throws RemoteException, InterruptedException {
        System.out.println("batchDeleteIntercept");
        return chain.batchDeleteMessage(accessList, request, timeoutMillis, communicationMode, callback);
    }

    @Override
    public PublishResult publishIntercept(List<String> accessList, Cmq.CMQProto request, long timeoutMillis,
                                          CommunicationMode communicationMode, PublishCallback publishCallback,
                                          int retryTimesWhenSendFailed, ProducerImpl producer, Chain chain)
            throws RemoteException, InterruptedException {
        System.out.println("publishIntercept");
        return chain.publishMessage(accessList, request, timeoutMillis, communicationMode, publishCallback,
                retryTimesWhenSendFailed, producer);
    }

    @Override
    public BatchPublishResult batchPublishIntercept(List<String> accessList, Cmq.CMQProto request, long timeoutMillis,
                                                    CommunicationMode communicationMode,
                                                    BatchPublishCallback publishCallback, int retryTimesWhenSendFailed,
                                                    ProducerImpl producer, Chain chain)
            throws RemoteException, InterruptedException {
        System.out.println("batchPublishIntercept");
        return chain.batchPublishMessage(accessList, request, timeoutMillis, communicationMode, publishCallback,
                retryTimesWhenSendFailed, producer);
    }
}
