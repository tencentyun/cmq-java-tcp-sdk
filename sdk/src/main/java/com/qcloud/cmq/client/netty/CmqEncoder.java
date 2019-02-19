package com.qcloud.cmq.client.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class CmqEncoder extends MessageToByteEncoder<ByteBuf> {

    private final static int headerLen = 6;
    private final static short soh = 0;

    @Override
    protected void encode(
            ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out) throws Exception {

        int bodyLen = msg.readableBytes();
        int totalLen = headerLen + bodyLen;
        out.ensureWritable(totalLen);
        out.writeShort(soh);
        // 大端转小端
        out.writeInt(Integer.reverseBytes(totalLen));
        out.writeBytes(msg, msg.readerIndex(), bodyLen);
    }
}