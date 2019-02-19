package demo;

import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.consumer.*;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;

public class ConsumerTransactionDemo {
    public static void main(String[] args) {
        final Consumer consumer = new Consumer();
        // 设置 Name Server地址，在控制台上获取， 必须设置
        consumer.setNameServerAddress("http://cmq-nameserver-gz.api.tencentyun.com");
        // 设置SecretId，在控制台上获取，必须设置
        consumer.setSecretId("AKIDvlSyJT3SM7g4HKFwzslXK0VqzxkNMGbq");
        // 设置SecretKey，在控制台上获取，必须设置
        consumer.setSecretKey("GMH2bzCF8qNju1znQtBIE0b1JRO9oJfr");
        // 设置签名方式，可以不设置，默认为SHA1
        consumer.setSignMethod(ClientConfig.SIGN_METHOD_SHA256);
        // 批量拉取时最大拉取消息数量，默认16
        consumer.setBatchPullNumber(32);
        // 设置没有消息时等待时间，默认10s。可在consumer.receiveMsg等方法中传入具体的等待时间
        consumer.setPollingWaitSeconds(6);
        // 设置请求超时时间， 默认3000ms
        // 如果设置了没有消息时等待时间为6s，超时时间为5000ms，则最终超时时间为(6*1000+5000)ms
        consumer.setRequestTimeoutMS(5000);

        // 消息拉取的队列名称
        final String queue = "treChannel_treGroup_1";

        // 启动消费者前必须设置好参数
        // 单条消息拉取，没有消息可消费时等待10s，不传入该参数则使用consumer设置的等待时间
        ReceiveResult result = null;
        try {
            // 启动消费者前必须设置好参数
            consumer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        while (true) {
            try {
                result = consumer.receiveMsg(queue, 10);
            } catch (MQClientException e) {
                e.printStackTrace();
            } catch (MQServerException e) {
                e.printStackTrace();
            }
            int ret = result.getReturnCode();
            if (ret == 0) {
                System.out.println("receive success, msgId:" + result.getMessage().getMessageId()
                        + " ReceiptHandle:" + result.getMessage().getReceiptHandle() + " Data:" + result.getMessage().getData());

                // 消费成功后确认消息。消息消费失败时，不用删除消息，消息会在一段时间后可再次被消费者拉取到
                // 异步确认消息
                try {
                    consumer.deleteMsg(queue, result.getMessage().getReceiptHandle(), new DeleteCallback() {
                        @Override
                        public void onSuccess(DeleteResult deleteResult) {
                            if (deleteResult.getReturnCode() != 0) {
                                System.out.println("delete msg error, ret:" + deleteResult.getReturnCode() + " ErrMsg:" + deleteResult.getErrorMessage());
                            } else {
                                System.out.println("delete msg success!");
                            }
                        }

                        @Override
                        public void onException(Throwable e) {
                            e.printStackTrace();
                            System.out.println("delete msg error: " + e);
                        }
                    });
                } catch (MQClientException e) {
                    e.printStackTrace();
                } catch (MQServerException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("receive Error, ret:" + ret + " ErrMsg:" + result.getErrorMessage());
            }

        }
    }
}
