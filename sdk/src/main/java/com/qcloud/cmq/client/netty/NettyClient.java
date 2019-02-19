package com.qcloud.cmq.client.netty;

import com.google.protobuf.TextFormat;
import com.qcloud.cmq.client.common.*;
import com.qcloud.cmq.client.client.CMQClientHandler;
import com.qcloud.cmq.client.protocol.Cmq;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NettyClient {
    private static final Logger logger = LogHelper.getLog();

    private static final long LOCK_TIMEOUT_MILLIS = 3000;

    private final NettyClientConfig nettyClientConfig;
    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroupWorker;
    private final Lock lockChannelTables = new ReentrantLock();
    private final ConcurrentMap<String /* address */, ChannelWrapper> channelTables = new ConcurrentHashMap<String, ChannelWrapper>();

    private final Timer timer = new Timer("ClientHouseKeepingService", true);
    private final CMQClientHandler clientHandler;
    private final ExecutorService executorService;
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    private final Semaphore semaphoreOneWay;
    private final Semaphore semaphoreAsync;
    private final ConcurrentMap<Long /* requestId */, ResponseFuture> responseTable = new ConcurrentHashMap<Long, ResponseFuture>(256);

    private Cmq.cmq_tcp_auth authData = null;

    public NettyClient(final NettyClientConfig nettyClientConfig, final CMQClientHandler handler) {
        this.semaphoreOneWay = new Semaphore(nettyClientConfig.getClientOnewaySemaphoreValue(), true);
        this.semaphoreAsync = new Semaphore(nettyClientConfig.getClientAsyncSemaphoreValue(), true);

        this.nettyClientConfig = nettyClientConfig;

        this.clientHandler = handler;
        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        this.executorService = Executors.newFixedThreadPool(publicThreadNums > 0 ? publicThreadNums : 4,
                new ThreadFactory() {
                    private AtomicInteger threadIndex = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
                    }
                });

        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });
    }

    public void setAuthData(Cmq.cmq_tcp_auth authData) {
        this.authData = authData;
    }

    private void processMessageReceived(ChannelHandlerContext ctx, Cmq.CMQProto cmd) throws Exception {
        if (cmd != null) {
            switch (cmd.getCmd()) {
                case Cmq.CMQ_CMD.CMQ_TRANSACTION_QUERY_VALUE:
                    logger.info("received transaction query");
                    processRequestCommand(ctx, cmd);
                    break;
                case Cmq.CMQ_CMD.CMQ_TCP_HEARTBEAT_VALUE:
                    break;
                case Cmq.CMQ_CMD.CMQ_TRANSACTION_CONFIRM_VALUE:
                    break;
                default:
                    processResponseCommand(ctx, cmd);
                    break;
            }
        }
    }

    private void dealConfirmAck(ChannelHandlerContext ctx, Cmq.CMQProto request){
        Cmq.cmq_transaction_confirm_reply reply = request.getTransactionConfirmReply();
        if (reply == null){
            logger.error("error in proto!");
        }
        // for single
        Cmq.cmq_transaction_confirm_reply_item item = reply.getItem(0);
        if (item.getState() != 1 || item.getErrCode() == 3){
            System.out.println("get error " + item.getErrMsg());
        }
    }


    private void processRequestCommand(final ChannelHandlerContext ctx, final Cmq.CMQProto cmd) {
        Runnable run = new Runnable() {
            @Override
            public void run() {
                final Cmq.CMQProto response = clientHandler.processRequest(ctx, cmd);
                if (response != null) {
                    try {
                        ctx.writeAndFlush(response);
                    } catch (Throwable e) {
                        logger.error("process request over, but response failed", e);
                        logger.error("request:{}", TextFormat.shortDebugString(cmd));
                        logger.error("response: {}", TextFormat.shortDebugString(response));
                    }
                }
            }
        };

        try {
            this.executorService.submit(run);
        } catch (RejectedExecutionException e) {
            if ((System.currentTimeMillis() % 10000) == 0) {
                logger.warn(RemoteHelper.parseChannelRemoteAddr(ctx.channel())
                        + ", too many requests and system thread pool busy, RejectedExecutionException "
                        + this.executorService.toString() + " request code: " + cmd.getCmd());
            }
        }
    }

    private void processResponseCommand(ChannelHandlerContext ctx, Cmq.CMQProto cmd) {
        final long requestId = cmd.getSeqno();
        final ResponseFuture responseFuture = responseTable.get(requestId);
        if (responseFuture != null) {
            if (LogHelper.LOG_REQUEST) {
                logger.debug("processResponseCommand:{}", TextFormat.shortDebugString(cmd));
            }
            responseFuture.setResponseCommand(cmd);
            responseFuture.release();
            responseTable.remove(requestId);
            if (responseFuture.getInvokeCallback() != null) {
                executeInvokeCallback(responseFuture);
            } else {
                responseFuture.putResponse(cmd);
            }
        } else {
            logger.warn("receive response, but not matched any request, " + RemoteHelper.parseChannelRemoteAddr(ctx.channel()));
            logger.warn("response:{}", TextFormat.shortDebugString(cmd));
        }
    }

    private void executeInvokeCallback(final ResponseFuture responseFuture) {
        boolean runInThisThread = false;
        ExecutorService executor = this.executorService;
        if (executor != null) {
            try {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            responseFuture.executeInvokeCallback();
                        } catch (Throwable e) {
                            logger.warn("execute callback in executor exception, and callback throw", e);
                        }
                    }
                });
            } catch (Exception e) {
                runInThisThread = true;
                logger.warn("execute callback in executor exception, maybe executor busy", e);
            }
        } else {
            runInThisThread = true;
        }

        if (runInThisThread) {
            try {
                responseFuture.executeInvokeCallback();
            } catch (Throwable e) {
                logger.warn("executeInvokeCallback Exception", e);
            }
        }
    }

    /**
     * This method is periodically invoked to scan and expire deprecated request.
     */
    private void scanResponseTable() {
        final List<ResponseFuture> rfList = new LinkedList<ResponseFuture>();
        Iterator<Map.Entry<Long, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();

            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                rep.release();
                it.remove();
                rfList.add(rep);
                logger.warn("remove timeout request, " + rep);
            }
        }

        for (ResponseFuture rf : rfList) {
            try {
                executeInvokeCallback(rf);
            } catch (Throwable e) {
                logger.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }

    private Cmq.CMQProto invokeSyncImpl(final Channel channel, final Cmq.CMQProto request, final long timeoutMillis)
            throws InterruptedException, RemoteSendRequestException, RemoteTimeoutException {
        if (LogHelper.LOG_REQUEST) {
            logger.debug("invokeSyncImpl: msg: " + TextFormat.shortDebugString(request) + " timeoutMillis:" + timeoutMillis);
        }
        final long requestId = request.getSeqno();
        try {
            final ResponseFuture responseFuture = new ResponseFuture(timeoutMillis, null, null);
            this.responseTable.put(requestId, responseFuture);
            final SocketAddress address = channel.remoteAddress();
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    }
                    responseFuture.setSendRequestOK(false);
                    responseTable.remove(requestId);
                    responseFuture.setCause(f.cause());
                    responseFuture.putResponse(null);
                    logger.warn("send a request command to channel <" + address + "> failed.");
                }
            });

            Cmq.CMQProto responseCommand = responseFuture.waitResponse(timeoutMillis);
            if (null == responseCommand) {
                if (responseFuture.isSendRequestOK()) {
                    throw new RemoteTimeoutException(RemoteHelper.parseSocketAddressAddr(address), timeoutMillis,
                            responseFuture.getCause());
                } else {
                    throw new RemoteSendRequestException(RemoteHelper.parseSocketAddressAddr(address), responseFuture.getCause());
                }
            }

            return responseCommand;
        } finally {
            this.responseTable.remove(requestId);
        }
    }

    private void invokeAsyncImpl(final Channel channel, final Cmq.CMQProto request, final long timeoutMillis,
                                 final InvokeCallback invokeCallback)
            throws InterruptedException, RemoteTooMuchRequestException, RemoteTimeoutException, RemoteSendRequestException {
        if (LogHelper.LOG_REQUEST) {
            logger.debug("invokeAsyncImpl: msg: " + TextFormat.shortDebugString(request) + " timeoutMillis:" + timeoutMillis);
        }
        final long requestId = request.getSeqno();
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);

            final ResponseFuture responseFuture = new ResponseFuture(timeoutMillis, invokeCallback, once);
            this.responseTable.put(requestId, responseFuture);
            try {
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        if (f.isSuccess()) {
                            responseFuture.setSendRequestOK(true);
                            return;
                        }
                        responseFuture.setSendRequestOK(false);
                        responseFuture.putResponse(null);
                        responseTable.remove(requestId);
                        try {
                            executeInvokeCallback(responseFuture);
                        } catch (Throwable e) {
                            logger.warn("execute callback in writeAndFlush addListener, and callback throw", e);
                        } finally {
                            responseFuture.release();
                        }
                        logger.warn("send a request command to channel <{}> failed.", RemoteHelper.parseChannelRemoteAddr(channel));
                    }
                });
            } catch (Exception e) {
                responseFuture.release();
                logger.warn("send a request command to channel <" + RemoteHelper.parseChannelRemoteAddr(channel) + "> Exception", e);
                throw new RemoteSendRequestException(RemoteHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemoteTooMuchRequestException("invokeAsyncImpl invoke too fast");
            } else {
                String info =
                        String.format("invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                                timeoutMillis, this.semaphoreAsync.getQueueLength(), this.semaphoreAsync.availablePermits());
                logger.warn(info);
                throw new RemoteTimeoutException(info);
            }
        }
    }

    private void invokeOneWayImpl(final Channel channel, final Cmq.CMQProto request, final long timeoutMillis)
            throws InterruptedException, RemoteTooMuchRequestException, RemoteTimeoutException, RemoteSendRequestException {
        if (LogHelper.LOG_REQUEST) {
            logger.debug("invokeOnyWayImpl: msg: " + TextFormat.shortDebugString(request) + " timeoutMillis:" + timeoutMillis);
        }
        boolean acquired = this.semaphoreOneWay.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneWay);
            try {
                Cmq.CMQProto newRequest = Cmq.CMQProto.newBuilder(request).build();
                channel.writeAndFlush(newRequest).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        once.release();
                        if (!f.isSuccess()) {
                            logger.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                        }
                    }
                });
            } catch (Exception e) {
                once.release();
                logger.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                throw new RemoteSendRequestException(RemoteHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemoteTooMuchRequestException("invokeOneWayImpl invoke too fast");
            } else {
                String info = String.format(
                        "invokeOneWayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                        timeoutMillis, this.semaphoreOneWay.getQueueLength(), this.semaphoreOneWay.availablePermits());
                logger.warn(info);
                throw new RemoteTimeoutException(info);
            }
        }
    }


    public Cmq.CMQProto invokeSync(List<String> addressList, final Cmq.CMQProto request, long timeoutMillis)
            throws InterruptedException, RemoteConnectException, RemoteSendRequestException, RemoteTimeoutException {
        Iterator<String> it = addressList.iterator();
        while (it.hasNext()) {
            String address = it.next();
            try {
                final ChannelWrapper cw = this.getAndCreateChannel(address);
                if (cw == null) {
                    throw new RemoteConnectException(address);
                }
                if (!cw.isLogin()) {
                    this.authChannel(cw);
                }
                final Channel channel = cw.getChannel();
                if (channel != null && channel.isActive()) {
                    try {
                        return this.invokeSyncImpl(channel, request, timeoutMillis);
                    } catch (RemoteSendRequestException e) {
                        logger.warn("invokeSync: send request exception, so close the channel[{}]", address);
                        this.closeChannel(address, channel);
                        throw e;
                    } catch (RemoteTimeoutException e) {
                        if (nettyClientConfig.isClientCloseSocketIfTimeout()) {
                            this.closeChannel(address, channel);
                            logger.warn("invokeSync: close socket because of timeout, {}ms, {}", timeoutMillis, address);
                        }
                        logger.warn("invokeSync: wait response timeout exception, the channel[{}]", address);
                        throw e;
                    }
                } else {
                    this.closeChannel(address, channel);
                    throw new RemoteConnectException(address);
                }
            } catch (RemoteConnectException e) {
                if (!it.hasNext()) {
                    throw e;
                }
            } catch (RemoteSendRequestException e) {
                if (!it.hasNext()) {
                    throw e;
                }
            }
        }
        return null;
    }

    public void invokeAsync(List<String> addressList, Cmq.CMQProto request, long timeoutMillis, InvokeCallback invokeCallback)
            throws InterruptedException, RemoteConnectException, RemoteTooMuchRequestException, RemoteTimeoutException,
            RemoteSendRequestException {
        Iterator<String> it = addressList.iterator();
        while (it.hasNext()) {
            String address = it.next();
            try {
                final ChannelWrapper cw = this.getAndCreateChannel(address);
                if (cw == null) {
                    throw new RemoteConnectException(address);
                }
                if (!cw.isLogin()) {
                    this.authChannel(cw);
                }
                final Channel channel = cw.getChannel();
                if (channel != null && channel.isActive()) {
                    try {
                        this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
                        return;
                    } catch (RemoteSendRequestException e) {
                        logger.warn("invokeAsync: send request exception, so close the channel[{}]", address);
                        this.closeChannel(address, channel);
                        throw e;
                    }
                } else {
                    this.closeChannel(address, channel);
                    throw new RemoteConnectException(address);
                }
            } catch (RemoteConnectException e) {
                if (!it.hasNext()) {
                    throw e;
                }
            } catch (RemoteSendRequestException e) {
                if (!it.hasNext()) {
                    throw e;
                }
            }
        }
    }

    public void invokeOneWay(List<String> addressList, Cmq.CMQProto request, long timeoutMillis) throws InterruptedException,
            RemoteConnectException, RemoteTooMuchRequestException, RemoteTimeoutException, RemoteSendRequestException {
        Iterator<String> it = addressList.iterator();
        while (it.hasNext()) {
            String address = it.next();
            try {
                final ChannelWrapper cw = this.getAndCreateChannel(address);
                if (cw == null) {
                    throw new RemoteConnectException(address);
                }
                if (!cw.isLogin()) {
                    this.authChannel(cw);
                }
                final Channel channel = cw.getChannel();
                if (channel != null && channel.isActive()) {
                    try {
                        this.invokeOneWayImpl(channel, request, timeoutMillis);
                        return;
                    } catch (RemoteSendRequestException e) {
                        logger.warn("invokeOneWay: send request exception, so close the channel[{}]", address);
                        this.closeChannel(address, channel);
                        throw e;
                    }
                } else {
                    this.closeChannel(address, channel);
                    throw new RemoteConnectException(address);
                }
            } catch (RemoteConnectException e) {
                if (!it.hasNext()) {
                    throw e;
                }
            } catch (RemoteSendRequestException e) {
                if (!it.hasNext()) {
                    throw e;
                }
            }
        }
    }


    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(nettyClientConfig.getClientWorkerThreads(),
                new ThreadFactory() {
                    private AtomicInteger threadIndex = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
                    }
                });

        this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)//
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
                .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize())
                .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                                defaultEventExecutorGroup,
                                // 编解码
                                new CmqEncoder(),
                                new ProtobufEncoder(),
                                new CmqDecoder(),
                                new ProtobufDecoder(Cmq.CMQProto.getDefaultInstance()),

                                new IdleStateHandler(0, 0,
                                        nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                                new NettyConnectManageHandler(),
                                new NettyClientHandler());
                    }
                });

        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    NettyClient.this.scanResponseTable();
                } catch (Exception e) {
                    logger.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);
    }

    public void shutdown() {
        try {
            this.timer.cancel();

            for (ChannelWrapper cw : this.channelTables.values()) {
                this.closeChannel(null, cw.getChannel());
            }

            this.channelTables.clear();

            this.eventLoopGroupWorker.shutdownGracefully();

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            logger.error("NettyClient shutdown exception, ", e);
        }

        if (this.executorService != null) {
            try {
                this.executorService.shutdown();
            } catch (Exception e) {
                logger.error("NettyRemoteServer shutdown exception, ", e);
            }
        }
    }

    private void closeChannel(final String address, final Channel channel) {
        if (null == channel)
            return;

        final String addressRemote = null == address ? RemoteHelper.parseChannelRemoteAddr(channel) : address;

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    final ChannelWrapper prevCW = this.channelTables.get(addressRemote);

                    logger.info("closeChannel: begin close the channel[{}] Found: {}", addressRemote, prevCW != null);

                    if (null == prevCW) {
                        logger.info("closeChannel: the channel[{}] has been removed from the channel table before", addressRemote);
                        removeItemFromTable = false;
                    } else if (prevCW.getChannel() != channel) {
                        logger.info("closeChannel: the channel[{}] has been closed before, and has been created again, nothing to do.",
                                addressRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addressRemote);
                        logger.info("closeChannel: the channel[{}] was removed from channel table", addressRemote);
                    }

                    RemoteHelper.closeChannel(channel);
                } catch (Exception e) {
                    logger.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                logger.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            logger.error("closeChannel exception", e);
        }
    }

    private void closeChannel(final Channel channel) {
        if (null == channel)
            return;

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    for (Map.Entry<String, ChannelWrapper> entry : channelTables.entrySet()) {
                        ChannelWrapper value = entry.getValue();
                        if (value.getChannel() != null && value.getChannel() == channel) {
                            this.channelTables.remove(entry.getKey());
                            logger.info("closeChannel: the channel[{}] was removed from channel table", entry.getKey());
                            RemoteHelper.closeChannel(channel);
                            break;
                        }
                    }
                } catch (Exception e) {
                    logger.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                logger.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            logger.error("closeChannel exception", e);
        }
    }

    private ChannelWrapper getAndCreateChannel(final String address) throws InterruptedException {
        ChannelWrapper cw = this.channelTables.get(address);
        if (cw != null && cw.isOK()) {
            return cw;
        }
        return this.createChannel(address);
    }

    private ChannelWrapper createChannel(final String address) throws InterruptedException {
        ChannelWrapper cw = this.channelTables.get(address);
        if (cw != null && cw.isOK()) {
            return cw;
        }

        if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                boolean createNewConnection;
                cw = this.channelTables.get(address);
                if (cw != null) {

                    if (cw.isOK()) {
                        return cw;
                    } else if (!cw.getChannelFuture().isDone()) {
                        createNewConnection = false;
                    } else {
                        this.channelTables.remove(address);
                        createNewConnection = true;
                    }
                } else {
                    createNewConnection = true;
                }

                if (createNewConnection) {
                    ChannelFuture channelFuture = this.bootstrap.connect(RemoteHelper.string2SocketAddress(address));
                    logger.info("createChannel: begin to connect remote host[{}] asynchronously", address);
                    cw = new ChannelWrapper(channelFuture);
                    this.channelTables.put(address, cw);
                }
            } catch (Exception e) {
                logger.error("createChannel: create channel exception", e);
            } finally {
                this.lockChannelTables.unlock();
            }
        } else {
            logger.warn("createChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        if (cw != null) {
            ChannelFuture channelFuture = cw.getChannelFuture();
            if (channelFuture.awaitUninterruptibly(this.nettyClientConfig.getConnectTimeoutMillis())) {
                if (cw.isOK()) {
                    logger.info("createChannel: connect remote host[{}] success, {}", address, channelFuture.toString());
                    return cw;
                } else {
                    logger.warn("createChannel: connect remote host[" + address + "] failed, " + channelFuture.toString(), channelFuture.cause());
                }
            } else {
                logger.warn("createChannel: connect remote host[{}] timeout {}ms, {}", address, this.nettyClientConfig.getConnectTimeoutMillis(),
                        channelFuture.toString());
            }
        }

        return null;
    }

    private void authChannel(ChannelWrapper cw) throws InterruptedException,
            RemoteTimeoutException, RemoteSendRequestException {
        if (this.authData == null) {
            throw new RemoteSendRequestException(RemoteHelper.parseSocketAddressAddr(cw.getChannel().remoteAddress()),
                    new Exception("Auth data is null"));
        }
        Cmq.CMQProto request = Cmq.CMQProto.newBuilder()
                .setCmd(Cmq.CMQ_CMD.CMQ_TCP_AUTH_VALUE)
                .setSeqno(RequestIdHelper.getNextSeqNo())
                .setTcpAuth(this.authData).build();
        Cmq.CMQProto response = this.invokeSyncImpl(cw.getChannel(), request, 2000);
        if (response.getResult() == ResponseCode.SUCCESS) {
            cw.setLogin();
        } else {
            throw new RemoteSendRequestException(RemoteHelper.parseSocketAddressAddr(cw.getChannel().remoteAddress()),
                    new Exception(response.getError()));
        }
    }

    static class ChannelWrapper {
        private final ChannelFuture channelFuture;
        private boolean isLogin = false;

        ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
        }

        boolean isOK() {
            return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
        }

        private Channel getChannel() {
            return this.channelFuture.channel();
        }

        ChannelFuture getChannelFuture() {
            return channelFuture;
        }

        boolean isLogin() {
            return isLogin;
        }

        void setLogin() {
            isLogin = true;
        }
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<Cmq.CMQProto> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Cmq.CMQProto msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
                            ChannelPromise promise) throws Exception {
            final String local = localAddress == null ? "UNKNOWN" : RemoteHelper.parseSocketAddressAddr(localAddress);
            final String remote = remoteAddress == null ? "UNKNOWN" : RemoteHelper.parseSocketAddressAddr(remoteAddress);
            logger.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);
            super.connect(ctx, remoteAddress, localAddress, promise);
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemoteHelper.parseChannelRemoteAddr(ctx.channel());
            logger.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            NettyClient.this.closeChannel(ctx.channel());
            super.disconnect(ctx, promise);
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemoteHelper.parseChannelRemoteAddr(ctx.channel());
            logger.info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
            NettyClient.this.closeChannel(ctx.channel());
            super.close(ctx, promise);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {

                    //构造并发送心跳包
                    Cmq.CMQProto.Builder heartBeatBuilder = Cmq.CMQProto.newBuilder();
                    heartBeatBuilder.setSeqno(RequestIdHelper.getNextSeqNo());
                    heartBeatBuilder.setCmd(Cmq.CMQ_CMD.CMQ_TCP_HEARTBEAT_VALUE);
                    ctx.channel().writeAndFlush(heartBeatBuilder);
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemoteHelper.parseChannelRemoteAddr(ctx.channel());
            logger.warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
            logger.warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
            NettyClient.this.closeChannel(ctx.channel());
        }
    }

}
