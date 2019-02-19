
import com.qcloud.cmq.client.common.LogHelper;
import com.qcloud.cmq.client.consumer.Consumer;
import com.qcloud.cmq.client.consumer.DeleteResult;
import com.qcloud.cmq.client.consumer.ReceiveResult;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;

public class ConsumerTest {
    public static void main(String args[]) {

        Consumer consumer = new Consumer();
        consumer.setSecretId("AKIDrcs5AKw5mKiutg9gxGOK5eEdppfLw7D7");
        consumer.setSecretKey("qV2NVSEPRGtMfqpcCaoqhMH14wc6TgiY");
        consumer.setNameServerAddress("http://cmq-nameserver-dev.api.qcloud.com");

        final String queue = "apfD1igRU";
        LogHelper.LOG_REQUEST = true;

        try {
            consumer.start();
            consumer.receiveMsg(queue);
            long begin = System.currentTimeMillis();
            ReceiveResult result = consumer.receiveMsg(queue);
            System.out.println("time: " + (System.currentTimeMillis() - begin));
            int ret = result.getReturnCode();
            if (ret == 0) {
                System.out.println("RecvMsg Success msgId:" + result.getMessage().getMessageId()
                        + " ReceiptHandle:" + result.getMessage().getReceiptHandle() + " Data:" + result.getMessage().getData());
                DeleteResult del_result = consumer.deleteMsg(queue, result.getMessage().getReceiptHandle());
                ret = del_result.getReturnCode();
                if (ret != 0) {
                    System.out.println("DeleteMsg Error, ret:" + ret + " ErrMsg:" + del_result.getErrorMessage());
                }
            } else {
                System.out.println("Recv Error, ret:" + ret + " ErrMsg:" + result.getErrorMessage());
            }

        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (MQServerException e) {
            e.printStackTrace();
        }

        try {
            Thread.sleep(1000);
            consumer.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}
