package demo;

import com.qcloud.cmq.client.common.ClientConfig;
import com.qcloud.cmq.client.consumer.Consumer;
import com.qcloud.cmq.client.consumer.Message;
import com.qcloud.cmq.client.consumer.MessageListener;

import java.util.ArrayList;
import java.util.List;

public class SubscribeDemo {
    public static void main(String args[]) {

        Consumer consumer = new Consumer();
        // 设置 Name Server地址。必须设置 不同地域不同网络不同 
        // 地域对应的缩写 bj:北京 sh：上海 gz:广州 in:孟买 ca:北美 cd:成都 cq: 重庆
        //  hk:香港 kr:韩国 ru:俄罗斯 sg:新加坡 shjr:上海金融 szjr:深圳金融 th:曼谷 use: 弗吉尼亚 usw： 美西 
        // 私有网络地址：http://cmq-nameserver-vpc-{region}.api.tencentyun.com 支持腾讯云私有网络的云服务器内网访问
        // 公网地址：    http://cmq-nameserver-{region}.tencentcloudapi.com
        consumer.setNameServerAddress("http://cmq-nameserver-xxx.tencentcloudapi.com");
        // 设置SecretId，在控制台上获取，必须设置
        consumer.setSecretId("xxx");
        // 设置SecretKey，在控制台上获取，必须设置
        consumer.setSecretKey("xxx");
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
        final String queue = "test-10";

        MessageListener listener = new MessageListener() {
            @Override
            public List<Long> consumeMessage(String queue, List<Message> msgs) {
                List<Long> ackList = new ArrayList<Long>();
                for (Message msg: msgs) {
                    // TODO 此处添加具体消费逻辑

                    System.out.println("queue:[" + queue + "] push msg:" + msg);
                    // 如果自己主动调用接口确认消息，则返回结果中不要包含该消息句柄
                    ackList.add(msg.getReceiptHandle());
                }
                return ackList;
            }
        };

        try {
            consumer.start();
            consumer.subscribe(queue, listener);

            System.out.println("Subscribe Success!");
        } catch (Exception e) {
            System.out.println("Subscribe Error:" + e);
        }
    }
}
