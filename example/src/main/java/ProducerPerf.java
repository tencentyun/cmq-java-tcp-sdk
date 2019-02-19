
import com.qcloud.cmq.client.common.ResponseCode;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;
import com.qcloud.cmq.client.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ProducerPerf {
    public static void main(String args[]) {

        Producer producer = new Producer();
        producer.setSecretId("AKIDrcs5AKw5mKiutg9gxGOK5eEdppfLw7D7");
        producer.setSecretKey("qV2NVSEPRGtMfqpcCaoqhMH14wc6TgiY");
        producer.setNameServerAddress("http://cmq-nameserver-shjr.api.qcloud.com");
        String queue = "apfD1igRU";

        try {
            producer.start();
            StringBuilder builder = new StringBuilder();
            int lenTime = Integer.parseInt(args[0])/8;
            for (int i = 0; i<lenTime; i++) {
                builder.append("12345678");
            }
            String msg = builder.toString();

            final AtomicLong count = new AtomicLong();
            final long begin = System.currentTimeMillis();
            while (true) {
                producer.send(queue, msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        long number = count.incrementAndGet();
                        if (number%1000 == 0) {
                            double qps = number * 1000.0 / (System.currentTimeMillis() - begin);
                            System.out.println("==> publish qps: " + qps);
                        }
                        if (sendResult.getReturnCode() != ResponseCode.SUCCESS) {
                            System.out.println("==> publish error result:" + sendResult);
                        }
                    }

                    @Override
                    public void onException(Throwable e) {
                        System.out.println("==> send error: ");
                        e.printStackTrace();
                    }
                });
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
