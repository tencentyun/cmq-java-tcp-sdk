package com.qcloud.cmq.client.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;

import java.util.List;

public class CmqDecoder extends ByteToMessageDecoder {

    private final static int headerLen = 6;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        in.markReaderIndex();
        if (in.readableBytes() < headerLen) {
            in.resetReaderIndex();
            return;
        }
        short soh = in.readShort();
        // 小端转大端
        int length = Integer.reverseBytes(in.readInt()) - headerLen;
        if (soh != 0 || length < 0) {
            throw new CorruptedFrameException("negative length: " + length);
        }

        if (in.readableBytes() < length) {
            in.resetReaderIndex();
        } else {
            out.add(in.readBytes(length));
        }
    }
}
