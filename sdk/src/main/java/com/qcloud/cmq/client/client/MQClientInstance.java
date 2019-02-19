package com.qcloud.cmq.client.client;

import com.qcloud.cmq.client.common.ServiceState;
import com.qcloud.cmq.client.common.*;
import com.qcloud.cmq.client.consumer.ConsumerImpl;
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;
import com.qcloud.cmq.client.producer.ProducerImpl;
import io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MQClientInstance {
    private final static long LOCK_TIMEOUT_MILLIS = 3000;
    private final Logger logger = LogHelper.getLog();
    private final ClientConfig clientConfig;
    private final String clientId;

    private final ConcurrentSet<ProducerImpl> producerTable = new ConcurrentSet<ProducerImpl>();
    private final ConcurrentSet<ConsumerImpl> consumerTable = new ConcurrentSet<ConsumerImpl>();

    private final ConcurrentHashMap<String, NameServerClient> nameServerTable = new ConcurrentHashMap<String, NameServerClient>();
    private final Lock lockNameServer = new ReentrantLock();

    private final CMQClient cMQClient;

    private ServiceState serviceState = ServiceState.CREATE_JUST;

    MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
        this.clientConfig = clientConfig;
        this.clientId = clientId;
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        nettyClientConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
        CMQClientHandler cMQClientHandler = new CMQClientHandler(this);
        this.cMQClient = new CMQClient(nettyClientConfig, cMQClientHandler, clientConfig, clientId);
        logger.info("created a new client Instance, FactoryIndex: {} ClientID: {} {} ",
            instanceIndex, this.clientId, this.clientConfig);
    }

    public void start() throws MQClientException {
        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    this.serviceState = ServiceState.START_FAILED;
                    this.checkConfig();
                    this.initNameServerClient();
                    this.cMQClient.start();
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case RUNNING:
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                case START_FAILED:
                    throw new MQClientException(ResponseCode.SYSTEM_ERROR,
                            "The client instance object[" + this.clientId + "] has been created before, and failed.");
                default:
                    break;
            }
        }
    }

    private void checkConfig() throws MQClientException {
        if (null == this.clientConfig.getNameServerAddress()) {
            throw new MQClientException("NameServer address is null", null);
        }
        if (null == this.clientConfig.getSecretId()) {
            throw new MQClientException("SecretID is null", null);
        }
        if (null == this.clientConfig.getSecretKey()) {
            throw new MQClientException("SecretKey is null", null);
        }
        if (null == this.clientConfig.getSignMethod()) {
            this.clientConfig.setSignMethod(ClientConfig.SIGN_METHOD_SHA1);
        } else if (!this.clientConfig.getSignMethod().equals(ClientConfig.SIGN_METHOD_SHA1)
                && !this.clientConfig.getSignMethod().equals(ClientConfig.SIGN_METHOD_SHA256)){
            throw new MQClientException(ResponseCode.SIGNATURE_METHOD_NOT_SUPPORT, "SignatureMethod must be HmacSHA1 or HmacSHA256");
        }
    }

    private void initNameServerClient() {
        String addressList = this.clientConfig.getNameServerAddress();
        String[] addressArray = addressList.split(";");
        List<String> list = Arrays.asList(addressArray);
        Collections.shuffle(list);
        for (String address : list) {
            logger.info("create a new name server client, address: {} ", address);
            this.nameServerTable.put(address, new NameServerClient(address, this.clientConfig));
        }
    }

    public void shutdown() {
        if (!this.consumerTable.isEmpty() || !this.producerTable.isEmpty()) {
            return;
        }
        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    break;
                case RUNNING:
                    this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                    this.cMQClient.shutdown();
                    MQClientManager.getInstance().removeClientFactory(this.clientId);
                    logger.info("the client instance [{}] shutdown OK", this.clientId);
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                default:
                    break;
            }
        }
    }

    public void updateQueueRoute(String queue, final ConcurrentHashMap<String, List<String>> routeTable) throws MQClientException {
        try {
            if (this.lockNameServer.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                for (NameServerClient client: this.nameServerTable.values()) {
                    try {
                        List<String> result = client.fetchQueueRoute(queue);
                        routeTable.put(queue, result);
                        break;
                    } catch (MQServerException e) {
                        logger.error("updateQueueRoute with Exception", e);
                    }
                }
                this.lockNameServer.unlock();
            } else {
                logger.warn("updateQueueRoute tryLock timeout {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            logger.warn("updateQueueRoute Exception", e);
        }
    }

    public void updateTopicRoute(String topic, final ConcurrentHashMap<String, List<String>> routeTable) throws MQClientException {
        try {
            if (this.lockNameServer.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                for (NameServerClient client: this.nameServerTable.values()) {
                    try {
                        List<String> result = client.fetchTopicRoute(topic);
                        routeTable.put(topic, result);
                        break;
                    } catch (MQServerException e) {
                        logger.error("updateTopicRoute with Exception", e);
                    }
                }
                this.lockNameServer.unlock();
            } else {
                logger.warn("updateTopicRoute tryLock timeout {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            logger.warn("updateTopicRoute Exception", e);
        }
    }

    public void registerConsumer(final ConsumerImpl consumer) {
        if (consumer != null) {
            this.consumerTable.add(consumer);
        }
    }

    public void unRegisterConsumer(final ConsumerImpl consumer) {
        this.consumerTable.remove(consumer);
    }

    public void registerProducer(final ProducerImpl producer) {
        if (producer != null) {
            this.producerTable.add(producer);
        }
    }

    public void unRegisterProducer(final ProducerImpl producer) {
        this.producerTable.remove(producer);
    }

    public CMQClient getCMQClient() {
        return cMQClient;
    }

}
