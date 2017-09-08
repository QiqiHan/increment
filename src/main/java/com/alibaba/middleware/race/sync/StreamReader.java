package com.alibaba.middleware.race.sync;

/**
 * Created by guojianpeng on 2017/6/18.
 */

import java.io.File;
import java.io.FileInputStream;

import java.io.IOException;

public class StreamReader {

    //文件句柄
    private FileInputStream  file;

    private long fileLength;
    private int lineLength = 0;
    private int stepLength = 54;

    //缓存区
    private int lineBufferSize = 1024;
    private byte[] lineBuffer = new byte[lineBufferSize];

    //指针
    private int readIndex = 0;
    private int lineIndex = 0;

    private Line l = new Line(null,0);



    public StreamReader(String s) {

        try {
            File f = new File(s);
            this.file = new FileInputStream (f);

            this.fileLength = f.length();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public Line getLine(){
        l.clearLine();

        if(readIndex >= fileLength)
            return null;

        try {
            file.skip(stepLength);
            readIndex += stepLength;
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        Byte temp = null;
        while(readIndex < fileLength) {
            try {
                temp = (byte)(file.read());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            if(temp == '\n')
                break;
            if(temp == 124)
                l.addindex(lineIndex);

            lineBuffer[lineIndex++] = temp;
            readIndex++;
            lineLength++;
        }

        l.setContents(lineBuffer);
        l.setIndex(lineLength);

        readIndex++;
        lineLength = 0;
        lineIndex = 0;


        return l;
    }




    public  static  void main(String[] args) throws IOException{
        long start = System.currentTimeMillis();

        StreamReader re = new StreamReader("/Users/mac/Desktop/canal_data/1.txt");

        Line line;


        while( (line = re.getLine())!=null){
            System.out.println(new String(line.getContents(),0,line.getIndex()));

        }
        long end = System.currentTimeMillis();
        System.out.println(""+(end-start)+"ms");
    }
}
