package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 处理client端的请求 Created by wanshao on 2017/5/25.
 */
public class ServerDemoInHandler extends ChannelInboundHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(Server.class);

//    private List<ColumnMeta> columnMetaList = new ArrayList<>();
    private HashMap<Integer,Integer> maps =new HashMap<>();
    int[] map = new int[20];
    private HashMap<Integer,Row> rows = new HashMap<>(100000);

    private BinlogReadUtil read;

    private int arrayindex = 0;

    private int metaLength = 0;
    /**
     * 根据channel
     * 
     * @param ctx
     * @return
     */
    public static String getIPString(ChannelHandlerContext ctx) {
        String ipString = "";
        String socketString = ctx.channel().remoteAddress().toString();
        int colonAt = socketString.indexOf(":");
        ipString = socketString.substring(1, colonAt);
        return ipString;
    }


    public ServerDemoInHandler(){
        try {
            read = new BinlogReadUtil(rows);
            String line = "";
            for (int i = 1; i <= 10; i++) {
                RandomAccessFile file = new RandomAccessFile(Constants.DATA_HOME + "/" + i + ".txt", "r");
                if (i == 1) {
                    line = file.readLine();
                }
                read.addFile(file);
            }
            Thread t = new Thread(read);
            t.start();

            byte[] bytes = line.getBytes();
            List<byte[]> bytes1 = read.split(bytes, 0);
            int length = bytes1.size();
            int index = 3;
            int count = 0;
            while (index < length) {
                maps.put(bytes1.get(index).length, count);
                map[bytes1.get(index).length] = count;
                count++;
                index = index + 3;
            }
            read.setMaps(maps);
            read.setMap(map);
//        metaLength = maps.size();
        }catch (Exception e){
            logger.info("" ,e);
        }
    }

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

        read.setChannel(channel);
//
//        read.run();
//        channel.writeAndFlush(byteBuf);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    private Object getMessage() throws InterruptedException {
        // 模拟下数据生成，每隔5秒产生一条消息
//        Thread.sleep(5000);
        return "message generated in ServerDemoInHandler";

    }
}
