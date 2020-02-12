package com.qcloud.cmq.client.common;

import com.qcloud.cmq.client.exception.MQClientException;
import io.netty.util.internal.StringUtil;

import java.util.Collection;

/**
 * @author: feynmanlin
 * @date: 2020/1/15 11:30 上午
 */
public class AssertUtil {
    private AssertUtil(){};

    public static void assertParamNotNull(Object obj,String errorMsg){
        if(obj == null){
            throw new MQClientException(ResponseCode.INVALID_REQUEST_PARAMETERS, errorMsg);
        }

        if(obj instanceof String && StringUtil.isNullOrEmpty((String)obj)){
            throw new MQClientException(ResponseCode.INVALID_REQUEST_PARAMETERS, errorMsg);
        }

        if(obj instanceof Collection && ((Collection) obj).isEmpty()){
            throw new MQClientException(ResponseCode.INVALID_REQUEST_PARAMETERS, errorMsg);
        }
    }
}

