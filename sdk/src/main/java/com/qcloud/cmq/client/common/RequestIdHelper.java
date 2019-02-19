package com.qcloud.cmq.client.common;


import org.slf4j.Logger;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * requestid设计，前10位为ip转为的int,中间5位为pid，后面再拼接上count。
 * 因为一个进程只会有一个netty连接，这样设计可以保证requestId不重复。
 */
public class RequestIdHelper {

    static private AtomicLong requestId = new AtomicLong(1);
    static private AtomicLong sno = new AtomicLong(1);
    private final static Logger logger = LogHelper.getLog();
    static private String prefix;

    // 构造ip的字符串
    static {
        InetAddress addr = null;
        try {
            addr = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            logger.error(e.getMessage());
        }
        int result = 0;
        byte[] bytes = addr.getAddress();
        for (int i = 0; i < bytes.length; i++) {
            result = result << 8 | bytes[i] & 0xff;
        }

        int pid = Integer.valueOf(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        String address = String.format("%010d", result);
        String pidStr = String.format("%05d", pid);
        prefix = address + pidStr;
    }

    public static long getRequestId() {
        return requestId.getAndIncrement();
    }

    public static long getNextSeqNo(){
        return sno.getAndIncrement();
    }
}
