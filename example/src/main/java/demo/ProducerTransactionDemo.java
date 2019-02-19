package demo;

import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.common.LogHelper;
import com.qcloud.cmq.client.common.ResponseCode;
import com.qcloud.cmq.client.common.TransactionStatus;
import com.qcloud.cmq.client.consumer.Message;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.producer.*;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class ProducerTransactionDemo {

    private final static Logger logger = LogHelper.getLog();

    public static void main(String[] args) {
        TransactionProducer producer = new TransactionProducer();
        // 设置 Name Server地址，在控制台上获取， 必须设置
        producer.setNameServerAddress("http://cmq-nameserver-dev.api.tencentyun.com");
        // 设置SecretId，在控制台上获取，必须设置
        producer.setSecretId("AKIDrcs5AKw5mKiutg9gxGOK5eEdppfLw7D7");
        // 设置SecretKey，在控制台上获取，必须设置
        producer.setSecretKey("qV2NVSEPRGtMfqpcCaoqhMH14wc6TgiY");
        // 设置签名方式，可以不设置，默认为SHA1
        producer.setSignMethod(ClientConfig.SIGN_METHOD_SHA256);
        // 设置发送消息失败时，重试的次数，设置为0表示不重试，默认为2
        producer.setRetryTimesWhenSendFailed(3);
        // 设置请求超时时间， 默认3000ms
        producer.setRequestTimeoutMS(5000);
        // 设置首次回查的时间间隔，默认5000ms
        producer.setFirstCheckInterval(5);
        // 消息发往的队列，在控制台创建
        String queue = "test_transaction";
        // 设置回查的回调函数，检查本地事务的校验结果
        producer.setChecker(queue, new TransactionStatusCheckerImpl());


        try{
            // 启动对象前必须设置好相关参数
            producer.start();
            String msg = "test_message";

            TransactionExecutor executor = new TransactionExecutor() {
                @Override
                public TransactionStatus execute(String msg, Object arg) {
                    //执行用户的本地事务 ... ...
                    System.out.println("do local transaction service!");
                    return TransactionStatus.SUCCESS;
                }
            };

            // 同步发送单条事务消息
            SendResult result = producer.sendTransactionMessage(queue, msg, executor, "test");

            if (result.getReturnCode() == ResponseCode.SUCCESS) {
                System.out.println("==> send success! msg_id:" + result.getMsgId() + " request_id:" + result.getRequestId());
            } else {
                System.out.println("==> code:" + result.getReturnCode() + " error:" + result.getErrorMsg());
            }

            // 同步发送批量事务消息
            // 每条事务消息对应一个本地事务
            List<TransactionExecutor> executorsList = new ArrayList<TransactionExecutor>();
            List<Object> argsList = new ArrayList<Object>();
            List<String> msgList = new ArrayList<String>();

            for (int i = 0;i < 3; i++){
                executorsList.add(executor);
                argsList.add("test_arg");
                msgList.add(msg + i);
            }

            BatchSendResult batchResult = producer.batchSendTransactionMessages(queue, msgList, executorsList, argsList);

            if (batchResult.getReturnCode() == ResponseCode.SUCCESS) {
                System.out.println("==> send success! msg_id:" + batchResult.getMsgIdList() + " request_id:" + batchResult.getRequestId());
            } else {
                System.out.println("==> code:" + batchResult.getReturnCode() + " error:" + batchResult.getErrorMessage());
            }

        }catch (MQClientException e){
            e.printStackTrace();
        }

    }
}

class TransactionStatusCheckerImpl implements TransactionStatusChecker{

    @Override
    public TransactionStatus checkStatus(Message msg) {
        //用户实现，检查本地事务的执行状态 ... ...
        return TransactionStatus.FAIL;
    }
}