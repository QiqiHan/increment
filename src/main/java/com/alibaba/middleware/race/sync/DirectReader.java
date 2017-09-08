package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by guojianpeng on 2017/6/12.
 */
public class DirectReader {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    //文件句柄
    private BufferedInputStream bis;



    private long fileLength;

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


    public DirectReader(String s) {


        try {
            File f = new File(s);
            FileInputStream  file = new FileInputStream (f);
            this.bis
                    = new BufferedInputStream (file);

            this.fileLength = f.length();
            this.end = Integer.parseInt(String.valueOf(fileLength));
            this.readIndex = end;
            this.start = Math.max(0,end-contentBufferSize);
            this.contentIndex = end-start-1;

            bis.mark(start);
            bis.read(contentBuffer,0,Math.min(contentBufferSize,end-start));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public Line getLastLine(){
        if(readIndex <= 0)
            return null;


        while(contentIndex>=0 && contentBuffer[contentIndex]!='\n') {
            lineBuffer[lineIndex--] = contentBuffer[contentIndex--];
            lineLength++;
        }


        //超出边界且文件未读完
        if(contentIndex<=0 && start!=0){

            end = readIndex;
            if(end<=0)
                return null;
            start =Math.max(0,end-contentBufferSize);
            try {
                bis.mark(start);
                bis.read(contentBuffer,0,Math.min(contentBufferSize,end-start));
                contentIndex = end - start -1;
                lineLength = 0;
                lineIndex = lineBufferSize-1;
            } catch (IOException e) {
                e.printStackTrace();
            }

            return getLastLine();
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

        DirectReader re = new DirectReader("D:\\项目\\syncdata\\canal.txt");
//        String s;
//        while((s = re.getLastLine())!=null)
//            System.out.println(s);
        Line line;
        while( (line = re.getLastLine())!=null && line.getIndex() >0){

            //System.out.println(new String(line.getContents(),line.getIndex(),1024-line.getIndex()));
//
//            parseBinlog = parseBinlog+end-start;
        }
        long end = System.currentTimeMillis();
        System.out.println(""+(end-start)+"ms");
    }
}
