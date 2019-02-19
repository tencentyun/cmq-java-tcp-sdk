
import com.qcloud.cmq.client.consumer.*;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;

import java.util.ArrayList;
import java.util.List;

public class ConsumerBatchTest {
    public static void main(String args[]) {

        Consumer consumer = new Consumer();
        consumer.setSecretId("AKIDrcs5AKw5mKiutg9gxGOK5eEdppfLw7D7");
        consumer.setSecretKey("qV2NVSEPRGtMfqpcCaoqhMH14wc6TgiY");
        consumer.setNameServerAddress("http://cmq-nameserver-shjr.api.qcloud.com");
        int ret;

        String queue = "apfD1igRU";
        int batch_num = 10;

        try {
            consumer.start();

            while (true) {
                BatchReceiveResult result = consumer.batchReceiveMsg(queue, batch_num);

                ret = result.getReturnCode();
                if (ret == 0) {
                    System.out.println("BatchRecvMsg Success! " + " request_id:" + result.getRequestId());
                    List<Message> msgList = result.getMessageList();
                    List<Long> receiptHandleArray = new ArrayList<Long>();
                    for (Message msg : msgList) {
                        System.out.println("msgId:" + msg.getMessageId() + " ReceiptHandle:" + msg.getReceiptHandle() + " Data:" + msg.getData());
                        receiptHandleArray.add(msg.getReceiptHandle());
                    }

                    BatchDeleteResult delResult = consumer.batchDeleteMsg(queue, receiptHandleArray);
                    ret = delResult.getReturnCode();
                    if (ret != 0) {
                        System.out.println("DeleteMsg Error, ret:" + ret + " ErrMsg:" + delResult.getErrorMessage());
                        List<ReceiptHandleErrorInfo> errorList = delResult.getErrorList();
                        for (ReceiptHandleErrorInfo info : errorList) {
                            System.out.println("ReceiptHandle:" + info.getReceiptHandle() + " RetCode:" + info.getReturnCode() + " ErrMsg:" + info.getErrorMessage());
                        }
                    }
                } else {
                    System.out.println("BatchRecv Error, ret:" + ret + " ErrMsg:" + result.getErrorMessage());
                }
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
