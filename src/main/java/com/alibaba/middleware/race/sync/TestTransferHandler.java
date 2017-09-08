package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Created by mac on 17/6/21.
 */
public class TestTransferHandler extends ChannelInboundHandlerAdapter {


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        // 保存channel
//        Server.getMap().put(getIPString(ctx), ctx.channel());
        ByteBuf result = (ByteBuf) msg;
        byte[] result1 = new byte[result.readableBytes()];
        // msg中存储的是ByteBuf类型的数据，把数据读取到byte[]中
        result.readBytes(result1);
//        String resultStr = new String(result1);
        Channel channel = ctx.channel();
//
//        read.setChannel(channel);
//
//        read.run();
//        channel.writeAndFlush(byteBuf);

        byte[] bytes = new byte[50*1024*1024];

        int i = 0;
        for(i = 0 ; i<40*1024*1024 ;i++){
            bytes[i] = 1;
        }

//        for(int j = 0 ; j<4 ; j++){
//            bytes[i] = 35;
//            i++;
//        }

        ByteBuf byteBuf =  Unpooled.wrappedBuffer(bytes);
        channel.writeAndFlush(byteBuf);

    }

}
