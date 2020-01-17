package demo;

import com.qcloud.cmq.client.cloudapi.CloudApiManager;
import com.qcloud.cmq.client.cloudapi.QueueMeta;
import com.qcloud.cmq.client.cloudapi.entity.CmqQueue;
import com.qcloud.cmq.client.common.CloudApiClientConfig;

import java.util.List;

/**
 * @author: feynmanlin
 * @date: 2020/1/15 12:17 下午
 */
public class CloudApiDemo {
    public static void main(String[] args) {
        CloudApiManager cloudApiManager = new CloudApiManager();
        /**
         * 【注意】这里是云API的地址，不是NameServer地址
         *  外网接口请求域名：https://cmq-queue-{$region}.api.qcloud.com
         *  内网接口请求域名：http://cmq-queue-{$region}.api.tencentyun.com
         *  上述域名中的{$region}需用具体地域替换：gz（广州）、sh（上海）、bj（北京）、shjr（上海金融）等
         */
        cloudApiManager.setCloudApiAddress("http://cmq-queue-gz.api.qcloud.com");
        cloudApiManager.setSecretId("");
        cloudApiManager.setSecretKey("");
        cloudApiManager.setSignMethod(CloudApiClientConfig.SIGN_METHOD_SHA1);
        cloudApiManager.setConnectTimeout(5000);

        cloudApiManager.start();

        int queueNum = cloudApiManager.countQueue("searchKeyWord",0,100);
        System.out.println("queue num :" + queueNum);

        List<CmqQueue> queueList = cloudApiManager.describeQueue("searchKeyWord",0,100);
        System.out.println("queue List :" + queueList.toString());

        QueueMeta queueMeta = new QueueMeta();
        queueMeta.setMaxMsgSize(1024);
        //cloudApiManager.createQueue("queueName",queueMeta);
    }
}
