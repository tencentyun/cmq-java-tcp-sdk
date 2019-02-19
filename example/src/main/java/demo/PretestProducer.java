package demo;


import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.common.LogHelper;
import com.qcloud.cmq.client.common.ResponseCode;
import com.qcloud.cmq.client.common.TransactionStatus;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.producer.BatchSendResult;
import com.qcloud.cmq.client.producer.SendResult;
import com.qcloud.cmq.client.producer.TransactionExecutor;
import com.qcloud.cmq.client.producer.TransactionProducer;
import org.slf4j.Logger;

// 50 threads , 50000 msg
public class PretestProducer {
    private final static Logger logger = LogHelper.getLog();
    public static int msgs = 0;

    public static void main(String[] args) {
        int count = Integer.valueOf(args[0]);
        msgs = Integer.valueOf(args[1]);
        for (int i = 0; i < count; i++) {
            Thread thread = new Thread(new SendTester());
            thread.start();
        }
    }
}

class MyTransactionExecutor implements TransactionExecutor{
    @Override
    public TransactionStatus execute(String msg, Object arg) {
        //用户的本地事务
        return TransactionStatus.SUCCESS;
    }
}

class SendTester implements Runnable{
    private final static Logger logger = LogHelper.getLog();
    @Override
    public void run() {
        String msg = "hello world";
        TransactionProducer producer = new TransactionProducer();
        // 设置 Name Server地址，在控制台上获取， 必须设置
        producer.setNameServerAddress("http://cmq-nameserver-dev.api.tencentyun.com");
        // 设置SecretId，在控制台上获取，必须设置
        producer.setSecretId("AKIDyG6vCyyAPnNEyA34Gft0AH4AW4k8Lqya");
        // 设置SecretKey，在控制台上获取，必须设置
        producer.setSecretKey("DZMQQXk4z1GtIXKEc1K7RamlviM7x7zb");
        // 设置签名方式，可以不设置，默认为SHA1
        producer.setSignMethod(ClientConfig.SIGN_METHOD_SHA256);
        // 设置发送消息失败时，重试的次数，设置为0表示不重试，默认为2
        producer.setRetryTimesWhenSendFailed(3);
        // 设置请求超时时间， 默认3000ms
        producer.setRequestTimeoutMS(5000);
        // 设置首次回查的时间间隔，默认5000ms
        producer.setFirstCheckInterval(5);
        // 消息发往的队列，在控制台创建
        String queue = "test_transcation";
        // 设置回查的回调函数，检查本地事务的校验结果
        producer.setChecker(queue, new TransactionStatusCheckerImpl());


        // 启动对象前必须设置好相关参数
        SendResult result = null;
        try {
            producer.start();
            TransactionExecutor executor = new MyTransactionExecutor();
            for (int i = 0;i < PretestProducer.msgs; i++){
                result = producer.sendTransactionMessage(queue, msg, executor, "test");
                if (result.getReturnCode() != ResponseCode.SUCCESS) {
                    System.out.println("==> code:" + result.getReturnCode() + " error:" + result.getErrorMsg());
                }
            }
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        System.out.println("thread send over!!!!");
        producer.shutdown();
    }
}
