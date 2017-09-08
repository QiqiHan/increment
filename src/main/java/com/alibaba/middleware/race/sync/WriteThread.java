package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedList;
import java.util.zip.Inflater;

/**
 * Created by mac on 17/6/18.
 */
public class WriteThread {

//    private static Logger logger = LoggerFactory.getLogger(Client.class);
//    private LinkedList<WriteObject> queue = new LinkedList<>();
////    byte[] buf = new byte[1024*6];
//    BufferedOutputStream bos ;
//
//    public WriteThread(LinkedList<WriteObject> queue){
//        try {
//            this.queue = queue;
//            bos = new BufferedOutputStream(new FileOutputStream(new File(Constants.RESULT_HOME), true));
//        }catch (Exception e){
//            logger.error("",e);
//        }
//    }
//
//    @Override
//    public void run() {
//
//
//        try {
//            while (!ClientDemoInHandler.isOver) {
//                WriteObject writeObject = queue.poll();
//                if (writeObject == null)
//                    continue;
//                bos.write(writeObject.getBytes(),0,writeObject.getIndex());
//            }
//
//            WriteObject writeObject = queue.poll();
//            while((writeObject=queue.poll())!=null){
//                bos.write(writeObject.getBytes(),0,writeObject.getIndex());
//            }
//
//            bos.flush();
//            bos.close();
//        }catch (Exception e){
//            logger.error("",e);
//        }
//    }
//
////    private byte[] decode(byte[] b){
////        Inflater decompresser = new Inflater();
////        decompresser.reset();
////        decompresser.setInput(b);
////        ByteArrayOutputStream o = new ByteArrayOutputStream();
////        try {
////            while (!decompresser.finished()) {
////                int i = decompresser.inflate(buf);
////                if(i==0){
////                    break;
////                }
////                o.write(buf, 0, i);
////            }
////            decompresser.end();
////
////        } catch (Exception e) {
////            e.printStackTrace();
////        } finally {
////            try {
////                o.close();
////            } catch (IOException e) {
////                e.printStackTrace();
////            }
////        }
////
////        decompresser.end();
////
////        return o.toByteArray();
////    }
}
