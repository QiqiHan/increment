package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by guojianpeng on 2017/6/15.
 */
public class InsertReader {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    //文件句柄
    private RandomAccessFile randomAccessFile;
    private FileChannel fc;
    private MappedByteBuffer currentContentBuffer = null;//当前buffer

    private long fileLength;
    private int lineLength = 0;

    //缓存区
    private int contentBufferSize = 40*1024*1024;
    //private int lineBufferSize = 1024;
    private byte[] contentBuffer = new byte[contentBufferSize];
    private int contentBufferLength;
    //private byte[] lineBuffer = new byte[lineBufferSize];

    //指针
    private int start = 0;
    private int readIndex = 0;
    private int contentIndex;
    //private int lineIndex = 0;
    private int ignoreLength = 0;
    private int insertLocation = 0;
    private Byte type;
    private boolean isStarted = false;

    public InsertReader(RandomAccessFile randomAccessFile) {
        this.randomAccessFile = randomAccessFile;
        try {
            this.fc = randomAccessFile.getChannel();
            this.fileLength = randomAccessFile.length();
            //this.end = Integer.parseInt(String.valueOf(fileLength));
            this.readIndex = 0;
            this.start = 0;
            this.contentIndex = 0;
            this.contentBufferLength = Math.min(contentBufferSize,Integer.parseInt(String.valueOf(fileLength))-start);
            this.currentContentBuffer = fc.map(FileChannel.MapMode.READ_ONLY,start, contentBufferLength);

            currentContentBuffer.get(contentBuffer,0, contentBufferLength);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public boolean getLine(){
        if(isStarted&&readIndex-insertLocation>40*1024*1024){
            //logger.info("t"+readIndex);
            System.out.println("t"+readIndex);
            insertLocation = readIndex;
            isStarted = false;
        }
        if (readIndex >= fileLength)
            return false;

        if (contentIndex + 54 < contentBufferLength) {
            ignoreLength = 54;
            contentIndex = contentIndex + 54;
            // System.out.println("60");
        }

        else if (contentIndex + 54 >= contentBufferLength) {
            // System.out.println("in");
            start = readIndex;
            if (start >= Integer.parseInt(String.valueOf(fileLength)))
                return false;

            try {
                this.contentBufferLength = Math.min(contentBufferSize,
                        Integer.parseInt(String.valueOf(fileLength)) - start);
                this.currentContentBuffer = fc.map(FileChannel.MapMode.READ_ONLY, start, contentBufferLength);
                currentContentBuffer.get(contentBuffer, 0, contentBufferLength);
                contentIndex = 0;
                lineLength = 0;
                //lineIndex = 0;
                ignoreLength = 0;

            } catch (IOException e) {
                e.printStackTrace();
            }

            return getLine();
        }

        while (contentIndex < contentBufferLength-1 && contentBuffer[contentIndex] != '\n') {
            if (contentBuffer[contentIndex++] == 124) {
                type = contentBuffer[contentIndex];
                ignoreLength++;
                if(type == 73){
                    if(isStarted){
                        if(readIndex - insertLocation > 1024){
                            //logger.info("t"+insertLocation);
                            //logger.info("s"+readIndex);
                            System.out.println("t"+insertLocation);
                            isStarted = false;
                        }
                        insertLocation = readIndex;
                    }
                    else{
                        isStarted = true;
                        insertLocation = readIndex;
                        System.out.println("s"+readIndex);
                    }


                    ignoreLength += 41;
                    contentIndex += 40;
                }
                else if(type == 85){
                    ignoreLength += 9;
                    contentIndex += 8;
                }
                else if(type == 68){
                    ignoreLength += 41;
                    contentIndex += 40;
                }
            }
            else {
                lineLength++;
            }
        }

        // 超出边界且文件未读完
        if (contentIndex >= contentBufferLength-1
                && start + contentBufferLength != Integer.parseInt(String.valueOf(fileLength))) {
            start = readIndex;
            if (start >= Integer.parseInt(String.valueOf(fileLength)))
                return false;

            try {
                this.contentBufferLength = Math.min(contentBufferSize,
                        Integer.parseInt(String.valueOf(fileLength)) - start);
                this.currentContentBuffer = fc.map(FileChannel.MapMode.READ_ONLY, start, contentBufferLength);
                currentContentBuffer.get(contentBuffer, 0, contentBufferLength);
                contentIndex = 0;
                lineLength = 0;
                //lineIndex = 0;
                ignoreLength = 0;

            } catch (IOException e) {
                e.printStackTrace();
            }

            return getLine();
        }



        readIndex = readIndex + ignoreLength + lineLength + 1;
        lineLength = 0;
        ignoreLength = 0;
        contentIndex++;

        return true;
    }

    public  static  void main(String[] args) throws IOException{
        long start = System.currentTimeMillis();
        RandomAccessFile raf = null;
        try {
            //raf = new RandomAccessFile("D:\\项目\\syncdata\\canal.txt","r");
            raf = new RandomAccessFile("D:\\项目\\syncdata\\canal_data\\2.txt","r");
            //raf = new RandomAccessFile("C:\\Users\\guojianpeng\\Desktop\\test\\line.txt","r");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        InsertReader re = new InsertReader(raf);



        while(re.getLine()){

            //System.out.println(new String(line.getContents(),0,line.getIndex()));
            /*if(line.getContents()[1]!=73 && line.getContents()[1]!=68 && line.getContents()[1]!=85){
                break;
            }*/
//
//            parseBinlog = parseBinlog+end-start;
        }
        long end = System.currentTimeMillis();
        System.out.println(""+(end-start)+"ms");
    }



}
