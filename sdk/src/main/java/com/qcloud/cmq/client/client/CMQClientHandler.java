package com.qcloud.cmq.client.client;

import com.qcloud.cmq.client.common.LogHelper;
import com.qcloud.cmq.client.common.RequestIdHelper;
import com.qcloud.cmq.client.consumer.Message;
import com.qcloud.cmq.client.producer.TransactionProducer;
import com.qcloud.cmq.client.producer.TransactionStatusChecker;
import com.qcloud.cmq.client.protocol.Cmq;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;

public class CMQClientHandler {
    private static final Logger logger = LogHelper.getLog();
    private final MQClientInstance cmqClientInstance;

    public CMQClientHandler(final MQClientInstance cmqClientInstance) {
        this.cmqClientInstance = cmqClientInstance;
    }

    public Cmq.CMQProto processRequest(ChannelHandlerContext ctx, Cmq.CMQProto request)  {

        switch(request.getCmd()){
            case Cmq.CMQ_CMD.CMQ_TRANSACTION_QUERY_VALUE:
                return this.checkTransactionStatus(ctx, request);
        }

        logger.error(" request type " + request.getCmd() + " not supported");
        return null;
    }

    public Cmq.CMQProto checkTransactionStatus(ChannelHandlerContext ctx, Cmq.CMQProto request){
        Cmq.cmq_transaction_query query = request.getTransactionQuery();

        Cmq.cmq_transaction_confirm.Builder confirmBuilder = Cmq.cmq_transaction_confirm.newBuilder();
        Cmq.cmq_transaction_confirm_item.Builder confirmItemBuilder = Cmq.cmq_transaction_confirm_item.newBuilder();

        Message message = new Message(query.getMsgId(), -1, null);
        confirmItemBuilder.setMsgId(message.getMessageId());
        TransactionStatusChecker checker = TransactionProducer.getChecker(query.getQueueName());
        if(null == checker){
            return null;
        }
        else{
            switch (checker.checkStatus(message)){
                case SUCCESS:
                    confirmItemBuilder.setState(1);
                    break;
                case FAIL:
                    confirmItemBuilder.setState(2);
                    break;
                case UN_KNOW:
                    return null;
                }

            confirmBuilder.addItem(confirmItemBuilder);
        }
        confirmBuilder.setQueueName(query.getQueueName());

        Cmq.CMQProto.Builder respBuilder = Cmq.CMQProto.newBuilder();
        respBuilder.setTransactionConfirm(confirmBuilder);
        respBuilder.setSeqno(RequestIdHelper.getNextSeqNo());
        respBuilder.setCmd(Cmq.CMQ_CMD.CMQ_TRANSACTION_CONFIRM_VALUE);
        return respBuilder.build();
    }
}
