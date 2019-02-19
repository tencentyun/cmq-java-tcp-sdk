
import com.qcloud.cmq.client.common.ResponseCode;
import com.qcloud.cmq.client.consumer.*;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;

import java.util.concurrent.atomic.AtomicLong;

public class ConsumerPerf {
    public static void main(String args[]) {

        final Consumer consumer = new Consumer();
        consumer.setSecretId("AKIDrcs5AKw5mKiutg9gxGOK5eEdppfLw7D7");
        consumer.setSecretKey("qV2NVSEPRGtMfqpcCaoqhMH14wc6TgiY");
        consumer.setNameServerAddress("http://cmq-nameserver-shjr.api.qcloud.com");

        final String queue = "apfD1igRU";
        try {
            consumer.start();
            final AtomicLong count = new AtomicLong();
            final long beginTime = System.currentTimeMillis();
            while (true) {
                consumer.receiveMsg(queue, new ReceiveCallback() {
                    @Override
                    public void onSuccess(final ReceiveResult result) {
                        int ret = result.getReturnCode();
                        if (ret == 0) {
                            long number = count.incrementAndGet();
                            if (number%1000 == 0) {
                                double qps = number * 1000.0 / (System.currentTimeMillis() - beginTime);
                                System.out.println("==> consume qps: " + qps);
                            }
                            try {
                                consumer.deleteMsg(queue, result.getMessage().getReceiptHandle(), new DeleteCallback() {
                                    @Override
                                    public void onSuccess(DeleteResult deleteResult) {
                                        int ret = deleteResult.getReturnCode();
                                        if (ret != 0) {
                                            System.out.println("DeleteMsg Error, ret:" + ret + " ErrMsg:" + deleteResult.getErrorMessage());
                                        }
                                    }

                                    @Override
                                    public void onException(Throwable e) {
                                        System.out.println("Delete Error");
                                        e.printStackTrace();
                                    }
                                });
                            } catch (MQClientException e) {
                                e.printStackTrace();
                            } catch (MQServerException e) {
                                e.printStackTrace();
                            }
                        } else if (ret != ResponseCode.NO_NEW_MESSAGES){
                            System.out.println("Receive Error, ret:" + ret + " ErrMsg:" + result.getErrorMessage());
                        }
                    }

                    @Override
                    public void onException(Throwable e) {
                        System.out.println("Receive Error");
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
            consumer.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}
