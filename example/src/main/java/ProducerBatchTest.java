
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;
import com.qcloud.cmq.client.producer.*;

import java.util.ArrayList;
import java.util.List;

public class ProducerBatchTest {
    public static void main(String args[]) {

        Producer producer = new Producer();
        producer.setSecretId("AKIDrcs5AKw5mKiutg9gxGOK5eEdppfLw7D7");
        producer.setSecretKey("qV2NVSEPRGtMfqpcCaoqhMH14wc6TgiY");
        producer.setNameServerAddress("http://cmq-nameserver-dev.api.qcloud.com");
        String queue = "apfD1igRU";
        try {
            producer.start();
            int i = 0;
            while (true) {
                final List<String> msgList = new ArrayList<String>();
                msgList.add("hello" + i++);
                msgList.add("world" + i++);
                producer.batchSend(queue, msgList, 1, new BatchSendCallback() {
                    @Override
                    public void onSuccess(BatchSendResult batchSendResult) {
                        System.out.println("==> publish success! msg:" + msgList + " result:" + batchSendResult);
                    }

                    @Override
                    public void onException(Throwable e) {
                        e.printStackTrace();
                        System.out.println("==> send error: " + e);
                    }
                });

                try {
                    Thread.sleep(1000);
                 } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
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
