package demo;

import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.common.LogHelper;
import com.qcloud.cmq.client.common.ResponseCode;
import com.qcloud.cmq.client.common.TransactionStatus;
import com.qcloud.cmq.client.consumer.Message;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;
import com.qcloud.cmq.client.producer.*;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ProducerDevTool {

    public final static Logger logger = LogHelper.getLog();
    public static TransactionStatus executorReturn;
    public static TransactionStatus checkerReturn;

    private static void printHelp(){
        System.out.println("error args!");
    }

    // single : java -jar producer.jar s test_transcation test_message 5 u s
    // batch  : java -jar example-1.0.0-jar-with-dependencies.jar b test_transcation test_message 5 3 2 1 s
    // producer : java -jar producer.jar p test4 test_msg 10
    public static void main(String[] args) {

        String queueName = "";
        int firstCheckInterval = 0;
        executorReturn = TransactionStatus.FAIL;
        checkerReturn = TransactionStatus.FAIL;
        String message = "";
        int successNum = 0;
        int failNum = 0;
        int unknowNum = 0;
        int producerNum = 0;

        // deal with arguments
        // valid check

        if(args.length != 6 && args.length != 8 && args.length != 4){
            printHelp();
            return ;
        }


        if(args[0].equals("s")){
            if (args.length != 6){
                printHelp();
                return ;
            }

            // queue name
            queueName = args[1];

            // message
            message = args[2];

            // firstCheckInterval
            firstCheckInterval = Integer.valueOf(args[3]);

            // executorReturn
            if (args[4].equals("s")){
                executorReturn = TransactionStatus.SUCCESS;
            }else if (args[4].equals("f")){
                executorReturn = TransactionStatus.FAIL;
            }else if (args[4].equals("u")){
                executorReturn = TransactionStatus.UN_KNOW;
            }else{
                printHelp();
                return;
            }

            // checkerReturn
            if (args[5].equals("s")){
                checkerReturn = TransactionStatus.SUCCESS;
            }else if (args[5].equals("f")){
                checkerReturn = TransactionStatus.FAIL;
            }else if (args[5].equals("u")){
                checkerReturn = TransactionStatus.UN_KNOW;
            }else{
                printHelp();
                return;
            }

        }else if(args[0].equals("b")){

            if (args.length != 8){
                printHelp();
                return ;
            }

            queueName = args[1];
            message = args[2];
            firstCheckInterval = Integer.valueOf(args[3]);
            successNum = Integer.valueOf(args[4]);
            failNum = Integer.valueOf(args[5]);
            unknowNum = Integer.valueOf(args[6]);
            // checkerReturn
            if (args[7].equals("s")){
                checkerReturn = TransactionStatus.SUCCESS;
            }else if (args[7].equals("f")){
                checkerReturn = TransactionStatus.FAIL;
            }else if (args[7].equals("u")){
                checkerReturn = TransactionStatus.UN_KNOW;
            }else{
                printHelp();
                return;
            }
        }else if(args[0].equals("p")){
            // producer : java -jar producer.jar p test4 test_msg 10
            queueName = args[1];
            message = args[2];
            producerNum = Integer.valueOf(args[3]);
        }
        else{
            printHelp();
            return;
        }

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
        producer.setFirstCheckInterval(firstCheckInterval);
        // 设置回查的回调函数，检查本地事务的校验结果
        producer.setChecker(queueName, new TransactionStatusCheckerDevImpl());
        // 消息发往的队列，在控制台创建
        String queue = queueName;

        try{
            // 启动对象前必须设置好相关参数
            producer.start();
            TransactionExecutor executorSucc = new TransactionExecutorSuccImpl();
            TransactionExecutor executorFail = new TransactionExecutorFailImpl();
            TransactionExecutor executorUnknow = new TransactionExecutorUnknowImpl();

            if (args[0].equals("s")){
                // 同步发送单条事务消息
                SendResult result = producer.sendTransactionMessage(queue, message, new TransactionExecutorImpl(), "test");

                if (result.getReturnCode() == ResponseCode.SUCCESS) {
                    System.out.println("==> send success! msg_id:" + result.getMsgId() + " request_id:" + result.getRequestId());
                } else {
                    System.out.println("==> code:" + result.getReturnCode() + " error:" + result.getErrorMsg());
                }
            }
            else if(args[0].equals("b")){
                // 同步发送多条事务消息
                List<TransactionExecutor> executorsList = new ArrayList<TransactionExecutor>();
                List<Object> argsList = new ArrayList<Object>();
                List<String> msgList = new ArrayList<String>();

                int msgCount = 0;
                for (int i = 0;i < successNum; i++){
                    executorsList.add(executorSucc);
                    argsList.add("test");
                    msgList.add(message + msgCount++);
                }
                for (int i = 0;i < failNum; i++){
                    executorsList.add(executorFail);
                    argsList.add("test");
                    msgList.add(message + msgCount++);
                }
                for (int i = 0;i < unknowNum; i++){
                    executorsList.add(executorUnknow);
                    argsList.add("test");
                    msgList.add(message + msgCount++);
                }
                if (executorsList.size() == 0){
                    return ;
                }

                BatchSendResult batchResult = producer.batchSendTransactionMessages(queue, msgList, executorsList, argsList);

                if (batchResult.getReturnCode() == ResponseCode.SUCCESS) {
                    System.out.println("==> send success! msg_id:" + batchResult.getMsgIdList() + " request_id:" + batchResult.getRequestId());
                } else {
                    System.out.println("==> code:" + batchResult.getReturnCode() + " error:" + batchResult.getErrorMessage());
                }
            }else if(args[0].equals("p")){
                List<String> list = new ArrayList<String>();
                for (int i = 0;i < producerNum; i++){
                    list.add(message + i);
                }
                BatchSendResult sendResult = producer.batchSend(queue, list);
                if (sendResult.getReturnCode() == ResponseCode.SUCCESS){
                    System.out.println("==> send success! msg id " + sendResult.getMsgIdList());
                }
            }
        }catch (MQClientException e){
            e.printStackTrace();
        } catch (MQServerException e) {
            e.printStackTrace();
        }
    }
}


class TransactionExecutorSuccImpl implements TransactionExecutor {

    @Override
    public TransactionStatus execute(String msg, Object arg) {
        //用户的本地事务
        System.out.println("do local transaction service!");
        return TransactionStatus.SUCCESS;
    }
}

class TransactionExecutorFailImpl implements TransactionExecutor {

    @Override
    public TransactionStatus execute(String msg, Object arg) {
        //用户的本地事务
        System.out.println("do local transaction service!");
        return TransactionStatus.FAIL;
    }
}

class TransactionExecutorUnknowImpl implements TransactionExecutor {

    @Override
    public TransactionStatus execute(String msg, Object arg) {
        //用户的本地事务
        System.out.println("do local transaction service!");
        return TransactionStatus.UN_KNOW;
    }
}

class TransactionExecutorImpl implements TransactionExecutor {

    @Override
    public TransactionStatus execute(String msg, Object arg) {
        //用户的本地事务
        System.out.println("do local transaction service!");
        return ProducerDevTool.executorReturn;
    }
}

class TransactionStatusCheckerDevImpl implements TransactionStatusChecker{

    @Override
    public TransactionStatus checkStatus(Message msg) {
        //用户检查本地事务的执行状态
        ProducerDevTool.logger.info("check!!!!!!!");
        return ProducerDevTool.checkerReturn;
    }
}