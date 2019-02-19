package demo;

import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.common.ResponseCode;
import com.qcloud.cmq.client.consumer.*;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;

import java.util.ArrayList;
import java.util.List;

public class PretestConsumer {
    public static void main(String[] args){


        if (args.length != 4 && args.length != 3){
            System.out.println("error args");
        }

        final String queueName = args[0];
        final String synOfAsyn = args[1];
        final String synOfAsynDel = args[2];
        int consumerNum = 0;
        if (args.length == 4)
            consumerNum = Integer.valueOf(args[3]);

        System.out.println(consumerNum);
        final Consumer consumer = new Consumer();
        // 设置 Name Server地址，在控制台上获取， 必须设置
        consumer.setNameServerAddress("http://10.11.151.96");
        // 设置SecretId，在控制台上获取，必须设置
        consumer.setSecretId("AKIDjBLCewWksJCoZcqgpM2crnOa42cqwWOt");
        // 设置SecretKey，在控制台上获取，必须设置
        consumer.setSecretKey("vYBIvFzxJLrxAXEcgODl1yd1HFwpDsfH");
        // 设置签名方式，可以不设置，默认为SHA1
        consumer.setSignMethod(ClientConfig.SIGN_METHOD_SHA256);
        // 批量拉取时最大拉取消息数量，默认16
        consumer.setBatchPullNumber(10);
        // 设置没有消息时等待时间，默认10s。可在consumer.receiveMsg等方法中传入具体的等待时间
        consumer.setPollingWaitSeconds(6);
        // 设置请求超时时间， 默认3000ms
        // 如果设置了没有消息时等待时间为6s，超时时间为5000ms，则最终超时时间为(6*1000+5000)ms
        consumer.setRequestTimeoutMS(5000);

        // 启动消费者前必须设置好参数
        try {
            consumer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        BatchReceiveResult receiveResult = null;
        if (synOfAsyn.equals("s")){

            if (consumerNum == 0) {
                try {
                    receiveResult = consumer.batchReceiveMsg(queueName);
                } catch (MQClientException e) {
                    e.printStackTrace();
                } catch (MQServerException e) {
                    e.printStackTrace();
                }
            }
            else {
                try {
                    receiveResult = consumer.batchReceiveMsg(queueName, consumerNum);
                } catch (MQClientException e) {
                    e.printStackTrace();
                } catch (MQServerException e) {
                    e.printStackTrace();
                }
            }

            if (receiveResult.getReturnCode() == ResponseCode.SUCCESS){
                System.out.println("consumer success!");
            }else {
                System.out.println("consumer fail " + receiveResult.getErrorMessage() + ":" + receiveResult.getReturnCode() + "consume num" +consumerNum);
                return ;
            }

            List<Long> handlerList = new ArrayList<Long>();
            for (Message msg : receiveResult.getMessageList()){
                handlerList.add(msg.getReceiptHandle());
            }

            if (synOfAsynDel.equals("s")){
                BatchDeleteResult result = null;
                try {
                    result = consumer.batchDeleteMsg(queueName, handlerList);
                } catch (MQClientException e) {
                    e.printStackTrace();
                } catch (MQServerException e) {
                    e.printStackTrace();
                }
                if (result.getReturnCode() == ResponseCode.SUCCESS){
                    System.out.println("==> delete message success!");
                }else {
                    System.out.println("consumer fail " + result.getErrorMessage() + ":" + result.getReturnCode());
                }
            }
            else if (synOfAsynDel.equals("a")){
                try {
                    consumer.batchDeleteMsg(queueName, handlerList, new BatchDeleteCallback() {
                        @Override
                        public void onSuccess(BatchDeleteResult deleteResult) {
                            System.out.println("==> delete message success!");
                        }

                        @Override
                        public void onException(Throwable e) {
                            System.out.println("==> delete message error");
                        }
                    });
                } catch (MQClientException e) {
                    e.printStackTrace();
                } catch (MQServerException e) {
                    e.printStackTrace();
                }
            }


        }else if (synOfAsyn.equals("a")){

                try {
                    consumer.batchReceiveMsg(queueName, new BatchReceiveCallback() {
                        @Override
                        public void onSuccess(BatchReceiveResult receiveResult) {

                            System.out.println("asyn consumer success!");
                            System.out.println(receiveResult.getErrorMessage());
                            System.out.println(receiveResult.getReturnCode());
                            List<Long> handlerList = new ArrayList<Long>();
                            for (Message msg : receiveResult.getMessageList()){
                                handlerList.add(msg.getReceiptHandle());
                            }

                            if (synOfAsynDel.equals("s")){
                                BatchDeleteResult result = null;
                                try {
                                    result = consumer.batchDeleteMsg(queueName, handlerList);
                                } catch (MQClientException e) {
                                    e.printStackTrace();
                                } catch (MQServerException e) {
                                    e.printStackTrace();
                                }
                                if (result.getReturnCode() == ResponseCode.SUCCESS){
                                    System.out.println("==> delete message success!");
                                }else {
                                    System.out.println("consumer fail " + result.getErrorMessage() + ":" + result.getReturnCode());
                                }
                            }
                            else if (synOfAsynDel.equals("a")){
                                try {
                                    consumer.batchDeleteMsg(queueName, handlerList, new BatchDeleteCallback() {
                                        @Override
                                        public void onSuccess(BatchDeleteResult deleteResult) {
                                            System.out.println("==> delete message success!");
                                        }

                                        @Override
                                        public void onException(Throwable e) {
                                            System.out.println("==> delete message error");
                                        }
                                    });
                                } catch (MQClientException e) {
                                    e.printStackTrace();
                                } catch (MQServerException e) {
                                    e.printStackTrace();
                                }
                            }
                        }

                        @Override
                        public void onException(Throwable e) {
                            System.out.println("asyn consumer fail!");
                        }
                    });
                } catch (MQClientException e) {
                    e.printStackTrace();
                } catch (MQServerException e) {
                    e.printStackTrace();
                }

            System.out.println("==> send asyn over!");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }else{
            System.out.println("error args!");
        }

    }
}
