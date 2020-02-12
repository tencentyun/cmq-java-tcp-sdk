package com.qcloud.cmq.client.client;

import com.qcloud.cmq.client.cloudapi.CloudApiQueueClient;
import com.qcloud.cmq.client.cloudapi.QueueMeta;
import com.qcloud.cmq.client.cloudapi.entity.CmqQueue;
import com.qcloud.cmq.client.common.AssertUtil;
import com.qcloud.cmq.client.common.CMQTool;
import com.qcloud.cmq.client.common.ResponseCode;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.http.CloudApiHttpClient;
import com.qcloud.cmq.client.http.HttpWrapper;
import com.qcloud.cmq.client.http.Json.JSONArray;
import com.qcloud.cmq.client.http.Json.JSONObject;
import io.netty.util.internal.StringUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * @author: feynmanlin
 * @date: 2020/1/14 5:12 下午
 */
public class CloudApiQueueClientV2 implements CloudApiQueueClient {

    private HttpWrapper cloudApiHttpUtil;

    public CloudApiQueueClientV2(CloudApiHttpClient cloudApiHttpClient) {
        this.cloudApiHttpUtil = cloudApiHttpClient;
    }

    @Override
    public String createQueue(String queueName, QueueMeta meta) {
        TreeMap<String, String> param = new TreeMap<String, String>();
        if (StringUtil.isNullOrEmpty(queueName)) {
            throw new MQClientException(ResponseCode.INVALID_REQUEST_PARAMETERS, "Invalid parameter:queueName is empty");
        } else {
            param.put("queueName", queueName);
        }

        if (meta.getMaxMsgHeapNum() > 0) {
            param.put("maxMsgHeapNum", Integer.toString(meta.getMaxMsgHeapNum()));
        }
        if (meta.getPollingWaitSeconds() > 0) {
            param.put("pollingWaitSeconds", Integer.toString(meta.getPollingWaitSeconds()));
        }
        if (meta.getVisibilityTimeout() > 0) {
            param.put("visibilityTimeout", Integer.toString(meta.getVisibilityTimeout()));
        }
        if (meta.getMaxMsgSize() > 0) {
            param.put("maxMsgSize", Integer.toString(meta.getMaxMsgSize()));
        }
        if (meta.getMsgRetentionSeconds() > 0) {
            param.put("msgRetentionSeconds", Integer.toString(meta.getMsgRetentionSeconds()));
        }
        if (meta.getRewindSeconds() > 0) {
            param.put("rewindSeconds", Integer.toString(meta.getRewindSeconds()));
        }

        String result = cloudApiHttpUtil.call("CreateQueue", param);
        CMQTool.checkResult(result);
        JSONObject jsonObject = new JSONObject(result);
        return jsonObject.getString("queueId");
    }

    @Override
    public int countQueue(String searchWord, int offset, int limit) {
        JSONObject jsonObj = ListQueue(searchWord, offset, limit);
        return jsonObj.getInt("totalCount");
    }

    private JSONObject ListQueue(String searchWord, int offset, int limit) {
        TreeMap<String, String> param = new TreeMap<String, String>();
        if (!StringUtil.isNullOrEmpty(searchWord)) {
            param.put("searchWord", searchWord);
        }
        if (offset >= 0) {
            param.put("offset", Integer.toString(offset));
        }
        if (limit > 0) {
            param.put("limit", Integer.toString(limit));
        }

        String result = cloudApiHttpUtil.call("ListQueue", param);
        CMQTool.checkResult(result);

        return new JSONObject(result);
    }

    @Override
    public List<CmqQueue> describeQueue(String searchWord, int offset, int limit) {
        JSONObject jsonObject = ListQueue(searchWord, offset, limit);
        JSONArray jsonArray = jsonObject.getJSONArray("queueList");
        List<CmqQueue> queueList = new ArrayList<>(jsonArray.length());
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = (JSONObject) jsonArray.get(i);
            CmqQueue cmqQueue = new CmqQueue();
            cmqQueue.setQueueId(obj.getString("queueId"));
            cmqQueue.setQueueName(obj.getString("queueName"));
            queueList.add(cmqQueue);
        }
        return queueList;
    }

    @Override
    public void deleteQueue(String queueName) {
        TreeMap<String, String> param = new TreeMap<String, String>();
        AssertUtil.assertParamNotNull(queueName, "Invalid parameter:topicName is empty");
        param.put("queueName", queueName);

        String result = cloudApiHttpUtil.call("DeleteQueue", param);
        CMQTool.checkResult(result);
    }

    @Override
    public QueueMeta getQueueAttributes(String queueName) {
        TreeMap<String, String> param = new TreeMap<String, String>();

        param.put("queueName", queueName);
        String result = cloudApiHttpUtil.call("GetQueueAttributes", param);
        JSONObject jsonObj = new JSONObject(result);
        CMQTool.checkResult(result);

        QueueMeta meta = new QueueMeta();
        meta.setMaxMsgHeapNum(jsonObj.getInt("maxMsgHeapNum"));
        meta.setPollingWaitSeconds(jsonObj.getInt("pollingWaitSeconds"));
        meta.setVisibilityTimeout(jsonObj.getInt("visibilityTimeout"));
        meta.setMaxMsgSize(jsonObj.getInt("maxMsgSize"));
        meta.setMsgRetentionSeconds(jsonObj.getInt("msgRetentionSeconds"));
        meta.setCreateTime(jsonObj.getInt("createTime"));
        meta.setLastModifyTime(jsonObj.getInt("lastModifyTime"));
        meta.setActiveMsgNum(jsonObj.getInt("activeMsgNum"));
        meta.setInactiveMsgNum(jsonObj.getInt("inactiveMsgNum"));
        meta.setRewindmsgNum(jsonObj.getInt("rewindMsgNum"));
        meta.setMinMsgTime(jsonObj.getInt("minMsgTime"));
        meta.setDelayMsgNum(jsonObj.getInt("delayMsgNum"));
        meta.setRewindSeconds(jsonObj.getInt("rewindSeconds"));

        return meta;
    }

}
