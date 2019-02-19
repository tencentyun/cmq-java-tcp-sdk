package demo;

import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.consumer.*;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;

import java.util.ArrayList;
import java.util.List;

public class ConsumerDemo {
    public static void main(String args[]) {

        final Consumer consumer = new Consumer();
        // 设置 Name Server地址，在控制台上获取， 必须设置
        consumer.setNameServerAddress("http://cmq-nameserver-region.api.qcloud.com");
        // 设置SecretId，在控制台上获取，必须设置
        consumer.setSecretId("SecretId");
        // 设置SecretKey，在控制台上获取，必须设置
        consumer.setSecretKey("SecretKey");
        // 设置签名方式，可以不设置，默认为SHA1
        consumer.setSignMethod(ClientConfig.SIGN_METHOD_SHA256);
        // 批量拉取时最大拉取消息数量，范围为1-16
        consumer.setBatchPullNumber(16);
        // 设置没有消息时等待时间，默认10s。可在consumer.receiveMsg等方法中传入具体的等待时间
        consumer.setPollingWaitSeconds(6);
        // 设置请求超时时间， 默认3000ms
        // 如果设置了没有消息时等待时间为6s，超时时间为5000ms，则最终超时时间为(6*1000+5000)ms
        consumer.setRequestTimeoutMS(5000);

        // 消息拉取的队列名称
        final String queue = "test-queue";

        try {
            // 启动消费者前必须设置好参数
            consumer.start();
            // 单条消息拉取，没有消息可消费时等待10s，不传入该参数则使用consumer设置的等待时间
            ReceiveResult result = consumer.receiveMsg(queue, 10);

            int ret = result.getReturnCode();
            if (ret == 0) {
                System.out.println("receive success, msgId:" + result.getMessage().getMessageId()
                        + " ReceiptHandle:" + result.getMessage().getReceiptHandle() + " Data:" + result.getMessage().getData());
                // TODO 此处写入消费逻辑

                // 消费成功后确认消息。消息消费失败时，不用删除消息，消息会在一段时间后可再次被消费者拉取到
                // 异步确认消息
                consumer.deleteMsg(queue, result.getMessage().getReceiptHandle(), new DeleteCallback() {
                    @Override
                    public void onSuccess(DeleteResult deleteResult) {
                        if (deleteResult.getReturnCode() != 0) {
                            System.out.println("delete msg error, ret:" + deleteResult.getReturnCode() + " ErrMsg:" + deleteResult.getErrorMessage());
                        }
                    }

                    @Override
                    public void onException(Throwable e) {
                        e.printStackTrace();
                        System.out.println("delete msg error: " + e);
                    }
                });
            } else {
                System.out.println("receive Error, ret:" + ret + " ErrMsg:" + result.getErrorMessage());
            }

            // 异步消费消息
            consumer.receiveMsg(queue, new ReceiveCallback() {
                @Override
                public void onSuccess(ReceiveResult receiveResult) {
                    // TODO 此处写入消费逻辑

                    try {
                        // 同步确认消息
                        DeleteResult del_result = consumer.deleteMsg(queue, receiveResult.getMessage().getReceiptHandle());
                        if (del_result.getReturnCode() != 0) {
                            System.out.println("delete msg error, ret:" + del_result.getReturnCode() + " ErrMsg:" + del_result.getErrorMessage());
                        }
                    } catch (MQClientException e) {
                        e.printStackTrace();
                    } catch (MQServerException e) {
                        e.printStackTrace();
                    }
                }
                @Override
                public void onException(Throwable e) {
                    e.printStackTrace();
                    System.out.println("receive msg error: " + e);
                }
            });

            // 批量拉取消息
            BatchReceiveResult batchResult = consumer.batchReceiveMsg(queue, 20);
            ret = batchResult.getReturnCode();
            if (ret == 0) {
                System.out.println("batch receive success! " + " request_id:" + batchResult.getRequestId());
                List<Message> msgList = batchResult.getMessageList();
                List<Long> receiptHandleArray = new ArrayList<Long>();
                for (Message msg : msgList) {
                    System.out.println("msgId:" + msg.getMessageId() + " ReceiptHandle:" + msg.getReceiptHandle() + " Data:" + msg.getData());
                    // TODO 此处添加真正的消费逻辑
                    receiptHandleArray.add(msg.getReceiptHandle());
                }

                BatchDeleteResult delResult = consumer.batchDeleteMsg(queue, receiptHandleArray);
                ret = delResult.getReturnCode();
                if (ret != 0) {
                    System.out.println("batch delete error, ret:" + ret + " ErrMsg:" + delResult.getErrorMessage());
                    List<ReceiptHandleErrorInfo> errorList = delResult.getErrorList();
                    for (ReceiptHandleErrorInfo info : errorList) {
                        System.out.println("ReceiptHandle:" + info.getReceiptHandle() + " RetCode:" + info.getReturnCode() + " ErrMsg:" + info.getErrorMessage());
                    }
                }
            } else {
                System.out.println("batch delete error, ret:" + ret + " ErrMsg:" + batchResult.getErrorMessage());
            }

            consumer.batchReceiveMsg(queue, new BatchReceiveCallback() {
                @Override
                public void onSuccess(BatchReceiveResult receiveResult) {
                    System.out.println("batch receive success! " + " request_id:" + receiveResult.getRequestId());
                    List<Message> msgList = receiveResult.getMessageList();
                    List<Long> receiptHandleArray = new ArrayList<Long>();
                    for (Message msg : msgList) {
                        System.out.println("msgId:" + msg.getMessageId() + " ReceiptHandle:" + msg.getReceiptHandle() + " Data:" + msg.getData());
                        // TODO 此处添加真正的消费逻辑
                        receiptHandleArray.add(msg.getReceiptHandle());
                    }

                    try {
                        // 批量确认消息
                        consumer.batchDeleteMsg(queue, receiptHandleArray, new BatchDeleteCallback() {
                            @Override
                            public void onSuccess(BatchDeleteResult deleteResult) {
                                if (deleteResult.getReturnCode() != 0) {
                                    System.out.println("batch delete error, ret:" + deleteResult.getReturnCode() + " ErrMsg:" + deleteResult.getErrorMessage());
                                    List<ReceiptHandleErrorInfo> errorList = deleteResult.getErrorList();
                                    for (ReceiptHandleErrorInfo info : errorList) {
                                        System.out.println("ReceiptHandle:" + info.getReceiptHandle() + " RetCode:" + info.getReturnCode() + " ErrMsg:" + info.getErrorMessage());
                                    }
                                }
                            }

                            @Override
                            public void onException(Throwable e) {

                            }
                        });
                    } catch (MQClientException e) {
                        e.printStackTrace();
                    } catch (MQServerException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void onException(Throwable e) {

                    e.printStackTrace();
                    System.out.println("batch receive msg error: " + e);
                }
            });
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
