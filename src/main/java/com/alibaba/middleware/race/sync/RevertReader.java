package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import static java.lang.Math.min;

/**
 * Created by guojianpeng on 2017/6/8.
 */
public class RevertReader {

    private static Logger logger = LoggerFactory.getLogger(Server.class);

    //文件句柄
    private RandomAccessFile randomAccessFile;
    private FileChannel fc;
    private MappedByteBuffer currentContentBuffer = null;//当前buffer

    private long fileLength;
    private String lineResult;
    private int lineLength = 0;

    //缓存区
    private int contentBufferSize = 20*1024*1024;
    private int lineBufferSize = 1024;
    private byte[] contentBuffer = new byte[contentBufferSize];
    private byte[] lineBuffer = new byte[lineBufferSize];

    //指针
    private int start = 0;
    private int end = 0;
    private int readIndex = 0;
    private int contentIndex;
    private int lineIndex = lineBufferSize-1;

    private Line l = new Line(null,0);
    //测试计时用
    public  long getStringTime() {
        return stringTime;
    }public void setStringTime(long stringTime) {
        this.stringTime = stringTime;
    }
    private long stringTime = 0l;
    private long readTime = 0l;
    private long copyTime = 0l;
    private long mapTime = 0l;
    private long cleanTime = 0l;


    private long startTime = 0l;
    private long endTime = 0l;

    public long getCleanTime() {
        return cleanTime;
    }

    public void setCleanTime(long cleanTime) {
        this.cleanTime = cleanTime;
    }
    public long getReadTime() {
        return readTime;
    }

    public long getCopyTime() {
        return copyTime;
    }

    public long getMapTime() {
        return mapTime;
    }

    public RevertReader(RandomAccessFile randomAccessFile) {
        this.randomAccessFile = randomAccessFile;
        try {
            this.fc = randomAccessFile.getChannel();
            this.fileLength = randomAccessFile.length();
            this.end = Integer.parseInt(String.valueOf(fileLength));
            this.readIndex = end;
            this.start = Math.max(0,end-contentBufferSize);
            this.contentIndex = end-start-1;

            this.currentContentBuffer = fc.map(FileChannel.MapMode.READ_ONLY,start,Math.min(contentBufferSize,end-start));

            currentContentBuffer.get(contentBuffer,0,Math.min(contentBufferSize,end-start));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public Line getLastLine(){
        if(readIndex <= 0)
            return null;

//        startTime = System.currentTimeMillis();

        //while(contentIndex>=0 && (temp = currentContentBuffer.get(contentIndex))!='\n') {
        while(contentIndex>=0 && contentBuffer[contentIndex]!='\n') {
                //System.out.println(lineIndex+" "+contentIndex+" "+(char)(contentBuffer[contentIndex]));
                lineBuffer[lineIndex--] = contentBuffer[contentIndex--];
                //lineBuffer[lineIndex--] = temp;
                //contentIndex--;
                lineLength++;
            }


        //超出边界且文件未读完
        if(contentIndex<=0 && start!=0){

            end = readIndex;
            if(end<=0)
                return null;
            start =Math.max(0,end-contentBufferSize);
            try {
                this.currentContentBuffer = fc.map(FileChannel.MapMode.READ_ONLY,start,Math.min(contentBufferSize,end-start));
                currentContentBuffer.get(contentBuffer,0,Math.min(contentBufferSize,end-start));
                contentIndex = end - start -1;
                lineLength = 0;
                lineIndex = lineBufferSize-1;
            } catch (IOException e) {
                e.printStackTrace();
            }

            return getLastLine();
        }
        else if(contentIndex<=0 && start == 0){
            return null;
        }

        l.setContents(lineBuffer);
        l.setIndex(lineBufferSize-lineLength);

        readIndex = readIndex - lineLength - 1;
        lineLength = 0;
        contentIndex--;


        lineIndex = lineBufferSize-1;


        return l;
    }




    public  static  void main(String[] args) throws IOException{
        long start = System.currentTimeMillis();
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile("D:\\项目\\syncdata\\canal_data\\10.txt","r");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        RevertReader re = new RevertReader(raf);

        Line line;

        while( (line = re.getLastLine())!=null && line.getIndex() >0){
            String s = new String(line.getContents(),line.getIndex(),1024-line.getIndex());
            System.out.println(s);
            if(!s.endsWith("|")&&!s.equals("")){
                break;
            }

        }
        long end = System.currentTimeMillis();
        System.out.println(""+(end-start)+"ms");
    }

}
