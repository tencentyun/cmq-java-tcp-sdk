
import com.qcloud.cmq.client.common.LogHelper;
import com.qcloud.cmq.client.common.ResponseCode;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;
import com.qcloud.cmq.client.producer.BatchPublishCallback;
import com.qcloud.cmq.client.producer.BatchPublishResult;
import com.qcloud.cmq.client.producer.Producer;

import java.util.ArrayList;
import java.util.List;

public class PublishBatchTest {
    public static void main(String args[]) {
//        LogHelper.LOG_REQUEST = true;
        Producer producer_1 = new Producer();
        producer_1.setSecretId("AKIDrcs5AKw5mKiutg9gxGOK5eEdppfLw7D7");
        producer_1.setSecretKey("qV2NVSEPRGtMfqpcCaoqhMH14wc6TgiY");
        producer_1.setNameServerAddress("http://cmq-nameserver-shjr.api.qcloud.com");

        Producer producer_2 = new Producer();
        producer_2.setSecretId("AKIDrcs5AKw5mKiutg9gxGOK5eEdppfLw7D7");
        producer_2.setSecretKey("qV2NVSEPRGtMfqpcCaoqhMH14wc6TgiY");
        producer_2.setNameServerAddress("http://cmq-nameserver-shjr.api.qcloud.com");

        String route_topic = "lambda-topic";
        String tag_topic = "lambda-topic";
        List<String> msg_list = new ArrayList<String>();
        msg_list.add("hello world1");
        msg_list.add("hello world2");
        try {
            producer_1.start();
            producer_2.start();
            while (true) {
                BatchPublishResult result = producer_1.batchPublish(route_topic, msg_list, "aa");
                if (result.getReturnCode() == ResponseCode.SUCCESS) {
                    System.out.println("SendMsg Success! " + " request_id:" + result.getRequestId());
                    List<Long> msgid_list = result.getMsgIdList();
                    for (Long aMsgid_list : msgid_list) {
                        System.out.println("MsgId:" + aMsgid_list);
                    }
                } else {
                    System.out.println("==> code:" + result.getReturnCode() + " error:" + result.getErrorMessage());
                }

                List<String> tagList = new ArrayList<String>();
                tagList.add("bb");
                tagList.add("cc");
                result = producer_2.batchPublish(tag_topic, msg_list, tagList);
                if (result.getReturnCode() == ResponseCode.SUCCESS) {
                    System.out.println("SendMsg Success! " + " request_id:" + result.getRequestId());
                    List<Long> msgid_list = result.getMsgIdList();
                    for (Long aMsgid_list : msgid_list) {
                        System.out.println("MsgId:" + aMsgid_list);
                    }
                } else {
                    System.out.println("==> code:" + result.getReturnCode() + " error:" + result.getErrorMessage());
                }

                producer_2.batchPublish(route_topic, msg_list, "dd", new BatchPublishCallback() {
                    @Override
                    public void onSuccess(BatchPublishResult result) {
                        if (result.getReturnCode() == ResponseCode.SUCCESS) {
                            System.out.println("SendMsg Success! " + " request_id:" + result.getRequestId());
                            List<Long> msgid_list = result.getMsgIdList();
                            for (Long aMsgid_list : msgid_list) {
                                System.out.println("MsgId:" + aMsgid_list);
                            }
                        } else {
                            System.out.println("==> code:" + result.getReturnCode() + " error:" + result.getErrorMessage());
                        }
                    }

                    @Override
                    public void onException(Throwable e) {
                        System.out.println("batch publish msg failed with error:" + e);
                    }
                });

                producer_1.batchPublish(tag_topic, msg_list, tagList, new BatchPublishCallback() {
                    @Override
                    public void onSuccess(BatchPublishResult result) {
                        if (result.getReturnCode() == ResponseCode.SUCCESS) {
                            System.out.println("SendMsg Success! " + " request_id:" + result.getRequestId());
                            List<Long> msgid_list = result.getMsgIdList();
                            for (Long aMsgid_list : msgid_list) {
                                System.out.println("MsgId:" + aMsgid_list);
                            }
                        } else {
                            System.out.println("==> code:" + result.getReturnCode() + " error:" + result.getErrorMessage());
                        }
                    }

                    @Override
                    public void onException(Throwable e) {
                        System.out.println("batch publish msg failed with error:" + e);
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
            producer_1.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
