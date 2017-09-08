package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedList;
import java.util.zip.Inflater;

/**
 * Created by wanshao on 2017/5/25.
 */
public class ClientDemoInHandler extends ChannelInboundHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(Client.class);

    private int num = 0;
//
//    private LinkedList<WriteObject> queue = new LinkedList<WriteObject>();
////
//    public static volatile boolean isOver = false;
//
////    private long zipsize = 0l;
//
//    private int count = 0;

    // 接收server端的消息，并打印出来
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        logger.info("get message ");
        ByteBuf result = (ByteBuf) msg;
        byte[] result1 = new byte[result.readableBytes()];
        result.readBytes(result1);

//        if(count<=1){
//            logger.info("size " +result1.length);
//            count++;
//        }
//
//        if(result1[result1.length-1] == 0 ){
//            if(result1[0] != 0){
//                int length = result1.length;
//                for(int i = result1.length-1 ; i>=0 ; i--){
//                    if(result1[i] !=0){
//                        break;
//                    }
//                    length--;
//                }
//                WriteObject writeObject = new WriteObject();
//                writeObject.setBytes(result1);
//                writeObject.setIndex(length);
//                queue.offer(writeObject);
//                isOver = true;
//            }else{
//                isOver = true;
//            }
//        }else{
//            WriteObject writeObject = new WriteObject();
//            writeObject.setBytes(result1);
//            writeObject.setIndex(result1.length);
//            queue.offer(writeObject);
//        }
//
//        if(isOver && queue.size() == 0){
//            result.release();
//            ctx.close();
//        }
        long st = System.currentTimeMillis();
        Inflater decompresser = new Inflater();
        decompresser.reset();
        decompresser.setInput(result1);
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        byte[] buf = new byte[1024*40];
        try {
            while (!decompresser.finished()) {
                int i = decompresser.inflate(buf);
                if(i==0){
                    break;
                }
                o.write(buf, 0, i);
            }
            buf = o.toByteArray();
            decompresser.end();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                o.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        decompresser.end();
        long et = System.currentTimeMillis();
        logger.info("unzip time is "+(et -st));

        st=System.currentTimeMillis();
        BufferedOutputStream bos =new BufferedOutputStream(new FileOutputStream(new File(Constants.RESULT_HOME)));
        bos.write(buf);
        et=System.currentTimeMillis();
        logger.info("write time is "+(et -st));

        bos.flush();
        bos.close();
        st = System.currentTimeMillis();
//        result.release();
//        ctx.close();
        System.exit(0);
//        et=System.currentTimeMillis();
//        logger.info("close time is "+(et -st));
    }

    // 连接成功后，向server发送消息
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
//        WriteThread writeThread = new WriteThread(this.queue);
//        Thread thread = new Thread(writeThread);
//        thread.start();
        logger.info("com.alibaba.middleware.race.sync.ClientDemoInHandler.channelActive");
        String msg = "send a message";
        ByteBuf encoded = ctx.alloc().buffer(4 * msg.length());
        encoded.writeBytes(msg.getBytes());
        ctx.write(encoded);
        ctx.flush();
    }
}
