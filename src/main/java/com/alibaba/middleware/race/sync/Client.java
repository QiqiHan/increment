package com.alibaba.middleware.race.sync;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by wanshao on 2017/5/25.
 */
public class Client {

    private final static int port = Constants.SERVER_PORT;
    Logger logger = LoggerFactory.getLogger(Client.class);

    // idle时间
    private static String ip;
    private EventLoopGroup loop = new NioEventLoopGroup();

    public static void main(String[] args) throws Exception {
        initProperties();
        // 从args获取server端的ip
        ip = args[0];
        Thread thread = new Thread(new SocketS());
        thread.start();
        Client client = new Client();
        client.connect(ip, port);
    }

    /**
     * 初始化系统属性
     */
    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
    }
    /**
     * 连接服务端
     *
     * @param host
     * @param port
     * @throws Exception
     */
    public void connect(String host, int port) throws Exception {
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        final ByteBuf delimeter = Unpooled.copiedBuffer(new byte[]{35,35,35,35});

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {

                @Override
                public void initChannel(SocketChannel ch) throws Exception {
//                    ch.pipeline().addLast(new LineBasedFrameDecoder(1024));
//                    ch.pipeline().addLast(new IdleStateHandler(10, 0, 0));

                    ch.pipeline().addLast(new DelimiterBasedFrameDecoder(1024*1024*40,delimeter));
//                    ch.pipeline().addLast(new ClientIdleEventHandler());
                    ch.pipeline().addLast(new ClientDemoInHandler());
                }
            });

            logger.info("client connect"+ ip+"  "+port);

            Thread.sleep(500);
            // Start the client.
            logger.info("start to connect ");

            ChannelFuture f;
            try {
                f = b.connect(host, port).sync();
                f.channel().closeFuture().sync();
            }catch (Exception e){
                Thread.sleep(500);
                f = b.connect(host, port).sync();
                f.channel().closeFuture().sync();
                logger.info("connect failed ");
            }
            // Wait until the connection is closed.

        } finally {
            workerGroup.shutdownGracefully();
        }

    }


}
