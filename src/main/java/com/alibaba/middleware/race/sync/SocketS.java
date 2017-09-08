package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by mac on 17/6/27.
 */
public class SocketS implements Runnable{

    Logger logger = LoggerFactory.getLogger(Client.class);

    @Override
    public void run() {
        try {
            //1、创建一个服务器端Socket，即ServerSocket，指定绑定的端口，并监听此端口
            ServerSocket serverSocket = new ServerSocket(5528);//1024-65535的某个端口
            //2、调用accept()方法开始监听，等待客户端的连接
            Socket socket = serverSocket.accept();
            //3、获取输入流，并读取客户端信息
            InputStream is = socket.getInputStream();
            byte[] bytes = new byte[40 * 1024 * 1024];
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(new File(Constants.RESULT_HOME)));
            int len = is.read(bytes);
            while(bytes[len-1] != 35) {
                bos.write(bytes, 0, len);
                len = is.read(bytes);
            }
            bos.write(bytes,0,len-1);
            socket.shutdownInput();//关闭输入流
            is.close();
            socket.close();
            serverSocket.close();
            bos.flush();
            bos.close();
            System.exit(0);
        }catch (Exception e){
            logger.error("error ", e);
        }
    }
}
