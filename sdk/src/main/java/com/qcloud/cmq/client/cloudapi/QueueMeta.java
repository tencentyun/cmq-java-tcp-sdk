package com.qcloud.cmq.client.cloudapi;

public class QueueMeta {
	
	/** 缺省消息接收长轮询等待时间 */
	public static final int DEFAULT_POLLING_WAIT_SECONDS = 0;
	/** 缺省消息可见性超时 */
	public static final int DEFAULT_VISIBILITY_TIMEOUT = 30;
	/** 缺省消息最大长度，单位字节 */
	public static final int DEFAULT_MAX_MSG_SIZE = 1048576;
	/** 缺省消息保留周期，单位秒 */
	public static final int DEFAULT_MSG_RETENTION_SECONDS = 345600;
	
	/** 最大堆积消息数 */
	private int maxMsgHeapNum = -1;
	/** 消息接收长轮询等待时间 */
	private int pollingWaitSeconds = DEFAULT_POLLING_WAIT_SECONDS;
	/** 消息可见性超时 */
	private int visibilityTimeout = DEFAULT_VISIBILITY_TIMEOUT;
	/** 消息最大长度 */
	private int maxMsgSize = DEFAULT_MAX_MSG_SIZE;
	/** 消息保留周期 */
	private int msgRetentionSeconds = DEFAULT_MSG_RETENTION_SECONDS;
	/** 队列创建时间 */
	private int createTime = -1;
	/** 队列属性最后修改时间 */
	private int lastModifyTime = -1;
	/** 队列处于Active状态的消息总数 */
	private int activeMsgNum = -1;
	/** 队列处于Inactive状态的消息总数 */
	private int inactiveMsgNum = -1;

	/** 已删除的消息，但还在回溯保留时间内的消息数量 */
	private int rewindmsgNum;
	/** 消息最小未消费时间 */
	private int minMsgTime;
	/** 延时消息数量 */
	private int delayMsgNum;
	
	/** 回溯时间 */
	private int rewindSeconds;

	public int getMaxMsgHeapNum() {
		return maxMsgHeapNum;
	}

	public void setMaxMsgHeapNum(int maxMsgHeapNum) {
		this.maxMsgHeapNum = maxMsgHeapNum;
	}

	public int getPollingWaitSeconds() {
		return pollingWaitSeconds;
	}

	public void setPollingWaitSeconds(int pollingWaitSeconds) {
		this.pollingWaitSeconds = pollingWaitSeconds;
	}

	public int getVisibilityTimeout() {
		return visibilityTimeout;
	}

	public void setVisibilityTimeout(int visibilityTimeout) {
		this.visibilityTimeout = visibilityTimeout;
	}

	public int getMaxMsgSize() {
		return maxMsgSize;
	}

	public void setMaxMsgSize(int maxMsgSize) {
		this.maxMsgSize = maxMsgSize;
	}

	public int getMsgRetentionSeconds() {
		return msgRetentionSeconds;
	}

	public void setMsgRetentionSeconds(int msgRetentionSeconds) {
		this.msgRetentionSeconds = msgRetentionSeconds;
	}

	public int getCreateTime() {
		return createTime;
	}

	public void setCreateTime(int createTime) {
		this.createTime = createTime;
	}

	public int getLastModifyTime() {
		return lastModifyTime;
	}

	public void setLastModifyTime(int lastModifyTime) {
		this.lastModifyTime = lastModifyTime;
	}

	public int getActiveMsgNum() {
		return activeMsgNum;
	}

	public void setActiveMsgNum(int activeMsgNum) {
		this.activeMsgNum = activeMsgNum;
	}

	public int getInactiveMsgNum() {
		return inactiveMsgNum;
	}

	public void setInactiveMsgNum(int inactiveMsgNum) {
		this.inactiveMsgNum = inactiveMsgNum;
	}

	public int getRewindmsgNum() {
		return rewindmsgNum;
	}

	public void setRewindmsgNum(int rewindmsgNum) {
		this.rewindmsgNum = rewindmsgNum;
	}

	public int getMinMsgTime() {
		return minMsgTime;
	}

	public void setMinMsgTime(int minMsgTime) {
		this.minMsgTime = minMsgTime;
	}

	public int getDelayMsgNum() {
		return delayMsgNum;
	}

	public void setDelayMsgNum(int delayMsgNum) {
		this.delayMsgNum = delayMsgNum;
	}

	public int getRewindSeconds() {
		return rewindSeconds;
	}

	public void setRewindSeconds(int rewindSeconds) {
		this.rewindSeconds = rewindSeconds;
	}
}
