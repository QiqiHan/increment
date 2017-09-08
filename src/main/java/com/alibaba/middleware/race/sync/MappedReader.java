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
public class MappedReader {
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
    private Line l = new Line(null,0);
    private int ignoreLength = 0;


    public MappedReader(RandomAccessFile randomAccessFile) {
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

    public Line getLine(){
        l.clearLine();
        //System.out.println(readIndex);
        if (readIndex >= fileLength)
            return null;

        if (contentIndex + 54 < contentBufferLength) {
            ignoreLength = 54;
            contentIndex = contentIndex + 54;
            // System.out.println("60");
        }

        else if (contentIndex + 54 >= contentBufferLength) {
            // System.out.println("in");
            start = readIndex;
            if (start >= Integer.parseInt(String.valueOf(fileLength)))
                return null;

            try {
                this.contentBufferLength = Math.min(contentBufferSize,
                        Integer.parseInt(String.valueOf(fileLength)) - start);
                this.currentContentBuffer = fc.map(FileChannel.MapMode.READ_ONLY, start, contentBufferLength);
                currentContentBuffer.get(contentBuffer, 0, contentBufferLength);
                contentIndex = 0;
                lineLength = 0;
                //lineIndex = 0;
                ignoreLength = 0;
                l.clearLine();
            } catch (IOException e) {
                e.printStackTrace();
            }

            return getLine();
        }

        while (contentIndex < contentBufferLength && contentBuffer[contentIndex] != '\n') {
            if (contentBuffer[contentIndex] == 124) {
                l.addindex(contentIndex);
                ignoreLength++;
            } else {
                lineLength++;
                if (contentBuffer[contentIndex] == 102) {
                    contentIndex += 13;
                    ignoreLength += 13;
                } else if (contentBuffer[contentIndex] == 108) {
                    contentIndex += 12;
                    ignoreLength += 12;
                } else if (contentBuffer[contentIndex] == 105) {
                    contentIndex += 5;
                    ignoreLength += 5;
                } else if (contentBuffer[contentIndex] == 78) {
                    contentIndex += 3;
                    ignoreLength += 3;
                } /*else if (contentBuffer[contentIndex] == 115 && contentBuffer[contentIndex + 1] == 101) {
                    lineLength++;
                    contentIndex += 6;
                    ignoreLength += 6;
                } else if (contentBuffer[contentIndex] == 115 && contentBuffer[contentIndex + 1] == 99) {
                    lineLength++;

                    if(contentBuffer[contentIndex + 5] != 58){
                        contentIndex ++;
                        ignoreLength ++;
                    }
                    contentIndex += 8;
                    ignoreLength += 8;
                }*/
            }

            contentIndex++;
        }

        // 超出边界且文件未读完
        if (contentIndex >= contentBufferLength
                && start + contentBufferLength != Integer.parseInt(String.valueOf(fileLength))) {
            start = readIndex;
            if (start >= Integer.parseInt(String.valueOf(fileLength)))
                return null;

            try {
                this.contentBufferLength = Math.min(contentBufferSize,
                        Integer.parseInt(String.valueOf(fileLength)) - start);
                this.currentContentBuffer = fc.map(FileChannel.MapMode.READ_ONLY, start, contentBufferLength);
                currentContentBuffer.get(contentBuffer, 0, contentBufferLength);
                contentIndex = 0;
                lineLength = 0;
                //lineIndex = 0;
                ignoreLength = 0;

                l.clearLine();
            } catch (IOException e) {
                e.printStackTrace();
            }

            return getLine();
        }

        l.setContents(contentBuffer);
        l.setIndex(contentIndex);

        readIndex = readIndex + ignoreLength + lineLength + 1;
        lineLength = 0;
        ignoreLength = 0;
        contentIndex++;
        //lineIndex = 0;

        return l;
    }




    public  static  void main(String[] args) throws IOException{
        long start = System.currentTimeMillis();
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile("D:\\项目\\syncdata\\canal.txt","r");
            //raf = new RandomAccessFile("C:\\Users\\guojianpeng\\Desktop\\test\\line.txt","r");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        MappedReader re = new MappedReader(raf);

        Line line;


        while( (line = re.getLine())!=null){

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
