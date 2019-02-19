
import com.qcloud.cmq.client.consumer.Consumer;
import com.qcloud.cmq.client.consumer.Message;
import com.qcloud.cmq.client.consumer.MessageListener;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;

import java.util.ArrayList;
import java.util.List;

public class SubscribeTest {
    public static void main(String args[]) {

        Consumer consumer = new Consumer();
        consumer.setSecretId("AKIDrcs5AKw5mKiutg9gxGOK5eEdppfLw7D7");
        consumer.setSecretKey("qV2NVSEPRGtMfqpcCaoqhMH14wc6TgiY");
        consumer.setNameServerAddress("http://cmq-nameserver-shjr.api.qcloud.com");
        String queue = "apfD1igRU";
        MessageListener listener = new MessageListener() {

            @Override
            public List<Long> consumeMessage(String queue, List<Message> msgs) {
                List<Long> ackList = new ArrayList<Long>();
                for (Message msg: msgs) {
                    System.out.println("queue:[" + queue + "] push msg:" + msg);
                    ackList.add(msg.getReceiptHandle());
                }
                return ackList;
            }
        };

        try {
            consumer.start();
            consumer.subscribe(queue, listener);
            System.out.println("Subscribe Success!");
        } catch (MQClientException e) {
            System.out.println("Subscribe Error:" + e);
        } catch (MQServerException e) {
            System.out.println("Subscribe Error:" + e);
        }
    }
}
