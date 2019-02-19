
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;
import com.qcloud.cmq.client.producer.*;

import java.util.ArrayList;
import java.util.List;

public class ProducerTest {
    public static void main(String args[]) {

        Producer producer = new Producer();
        producer.setSecretId("AKIDvlSyJT3SM7g4HKFwzslXK0VqzxkNMGbq");
        producer.setSecretKey("GMH2bzCF8qNju1znQtBIE0b1JRO9oJfr");
        producer.setNameServerAddress("http://cmq-nameserver-gz.api.tencentyun.com");
        String queue = "queue-test10";
        try {
            producer.start();
            final String msg = "hello world";
            SendResult result = producer.send(queue, msg);
            int ret = result.getReturnCode();
            if (ret == 0) {
                System.out.println("==> SendMsg Success! msg_id:" + result.getMsgId() + " request_id:" + result.getRequestId());
            } else {
                System.out.println("==> ret:" + ret + " ErrMsg:" + result.getErrorMsg());
            }

            result = producer.send(queue, msg, 10);
            System.out.println("==> publish success! msg:" + msg + " result:" + result);

            producer.send(queue, msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println("==> publish success! msg:" + msg + " result:" + sendResult);
                }

                @Override
                public void onException(Throwable e) {
                    e.printStackTrace();
                    System.out.println("==> send error: " + e);
                }
            });
            final List<String> msgList = new ArrayList<String>();
            msgList.add("hello");
            msgList.add("world");
            producer.batchSend(queue, msgList, 1, new BatchSendCallback() {
                @Override
                public void onSuccess(BatchSendResult result) {
                    int ret = result.getReturnCode();
                    if (ret == 0) {
                        System.out.println("==> SendMsg Success! msg_id:" + result.getMsgIdList() + " request_id:" + result.getRequestId());
                    } else {
                        System.out.println("==> ret:" + ret + " ErrMsg:" + result.getErrorMessage());
                    }
                }
                @Override
                public void onException(Throwable e) {
                    e.printStackTrace();
                    System.out.println("==> send error: " + e);
                }
            });

        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (MQServerException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(1000);
            producer.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
