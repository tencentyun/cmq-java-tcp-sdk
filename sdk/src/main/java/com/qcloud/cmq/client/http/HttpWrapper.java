package com.qcloud.cmq.client.http;

import java.util.TreeMap;

/**
 * @author: feynmanlin
 * @date: 2020/1/17 11:42 上午
 */
public interface HttpWrapper {
    String call(String action, TreeMap<String, String> param);
}
