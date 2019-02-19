package demo;

import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;
import com.qcloud.cmq.client.producer.*;

import java.util.ArrayList;
import java.util.List;

public class PublishDemo {
    public static void main(String args[]) throws MQClientException, MQServerException {

        Producer producer = new Producer();
        // 设置 Name Server地址，在控制台上获取， 必须设置
        producer.setNameServerAddress("http://10.11.151.96");
        // 设置SecretId，在控制台上获取，必须设置
        producer.setSecretId("AKIDjBLCewWksJCoZcqgpM2crnOa42cqwWOt");
        // 设置SecretKey，在控制台上获取，必须设置
        producer.setSecretKey("vYBIvFzxJLrxAXEcgODl1yd1HFwpDsfH");
        // 设置签名方式，可以不设置，默认为SHA1
        producer.setSignMethod(ClientConfig.SIGN_METHOD_SHA256);
        // 设置发送消息失败时，重试的次数，设置为0表示不重试，默认为2
        producer.setRetryTimesWhenSendFailed(3);
        // 设置请求超时时间， 默认3000ms
        producer.setRequestTimeoutMS(5000);

        // 消息发往的Topic，在控制台分别创建两种模式的Topic
        String route_topic = args[0];
        String route_key = args[1];
        String tag_topic = "tag-topic";

        producer.start();

        // 带routeKey发送消息
        String msg = "msg with routeKey:tre";
        PublishResult result = producer.publish(route_topic, msg, route_key);
        System.out.println("==> publish msg:" + msg + " result:" + result);

        // 带tag发送消息
//        msg = "publish msg with tag[bb, cc]";
//        List<String> tagList = new ArrayList<String>();
//        tagList.add("bb");
//        tagList.add("cc");
//        result = producer.publish(tag_topic, msg, tagList);
//        System.out.println("==> publish msg:" + msg + " result:" + result);

        // 带routeKey异步发送消息
        final String routeMsg = "publish msg with routeKey:aa ";
        producer.publish(route_topic, routeMsg, "aa", new PublishCallback() {
            @Override
            public void onSuccess(PublishResult publishResult) {
                System.out.println("==> publish msg:" + routeMsg + " result:" + publishResult);
            }

            @Override
            public void onException(Throwable e) {
                System.out.println("==> publish msg:" + routeMsg + " failed with error:" + e);
            }
        });

        // 带tag异步发送消息
//        final String tagMsg = "publish msg with tag[bb, cc]";
//        producer.publish(tag_topic, tagMsg, tagList, new PublishCallback() {
//            @Override
//            public void onSuccess(PublishResult publishResult) {
//                System.out.println("==> publish msg:" + tagMsg + " result:" + publishResult);
//            }
//
//            @Override
//            public void onException(Throwable e) {
//                System.out.println("==> publish msg:" + tagMsg + " failed with error:" + e);
//            }
//        });

//        List<String> msg_list = new ArrayList<String>();
//        msg_list.add("hello world1");
//        msg_list.add("hello world2");
//        // 带routeKey批量发送消息
//        BatchPublishResult batchResult = producer.batchPublish(route_topic, msg_list, "aa");
//        List<Long> msgid_list = batchResult.getMsgIdList();
//        System.out.println("batch publish success! " + " request_id:" + batchResult.getRequestId());
//        for (Long aMsgid_list : msgid_list) {
//            System.out.println("MsgId:" + aMsgid_list);
//        }
        // 带tag批量发送消息
//        batchResult = producer.batchPublish(tag_topic, msg_list, tagList);
//        msgid_list = batchResult.getMsgIdList();
//        System.out.println("batch publish success! " + " request_id:" + batchResult.getRequestId());
//        for (Long aMsgid_list : msgid_list) {
//            System.out.println("MsgId:" + aMsgid_list);
//        }

        // 带routeKey异步批量发送消息
//        producer.batchPublish(route_topic, msg_list, "aa", new BatchPublishCallback() {
//            @Override
//            public void onSuccess(BatchPublishResult batchPublishResult) {
//                List<Long> msgid_list = batchPublishResult.getMsgIdList();
//                System.out.println("batch publish success! " + " request_id:" + batchPublishResult.getRequestId());
//                for (Long aMsgid_list : msgid_list) {
//                    System.out.println("MsgId:" + aMsgid_list);
//                }
//            }
//
//            @Override
//            public void onException(Throwable e) {
//                System.out.println("batch publish msg failed with error:" + e);
//            }
//        });
        // 带tag异步批量发送消息
//        producer.batchPublish(tag_topic, msg_list, tagList, new BatchPublishCallback() {
//            @Override
//            public void onSuccess(BatchPublishResult batchPublishResult) {
//                List<Long> msgid_list = batchPublishResult.getMsgIdList();
//                System.out.println("batch publish success! " + " request_id:" + batchPublishResult.getRequestId());
//                for (Long aMsgid_list : msgid_list) {
//                    System.out.println("MsgId:" + aMsgid_list);
//                }
//            }
//
//            @Override
//            public void onException(Throwable e) {
//                System.out.println("batch publish msg failed with error:" + e);
//            }
//        });

        try {
            Thread.sleep(1000);
            producer.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
