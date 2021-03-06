package com.alibaba.middleware.race.sync;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by guojianpeng on 2017/6/8.
 */
public class BufferWriter {
    private RandomAccessFile randomAccessFile;
    private long bufferSize = 8*1024*1024;
    private MappedByteBuffer contentBuffer = null;
    private FileChannel fc;
    private Long base = 0L;
    private Long currentLength = 0l;

    public BufferWriter(String fileName) {
        try {
            randomAccessFile = new RandomAccessFile(fileName, "rw");
            fc = randomAccessFile.getChannel();
            contentBuffer = fc.map(FileChannel.MapMode.READ_WRITE,0,bufferSize);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public BufferWriter(RandomAccessFile r){
        this.randomAccessFile =r;
        try {
            fc = randomAccessFile.getChannel();
            contentBuffer = fc.map(FileChannel.MapMode.READ_WRITE,0,bufferSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void putContent(byte[] bufferArray, int offset, int length){
        long writeOffset = currentLength - base;
        try {
            if(writeOffset+length>bufferSize){
                contentBuffer = fc.map(FileChannel.MapMode.READ_WRITE,currentLength,bufferSize);
                base = currentLength;
                contentBuffer.put(bufferArray,offset,length);
            }
            else{
                contentBuffer.put(bufferArray,offset,length);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        currentLength = currentLength+length;
    }

    public void putContent(String s){
        long writeOffset = currentLength - base;
        try {
            if(writeOffset+s.length()>bufferSize){
                contentBuffer = fc.map(FileChannel.MapMode.READ_WRITE,currentLength,bufferSize);
                base = currentLength;
                contentBuffer.put(s.getBytes(),0,s.length());
            }
            else{
                contentBuffer.put(s.getBytes(),0,s.length());
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        currentLength = currentLength+s.length();
    }

    public void closeWriter(){
        try {
            randomAccessFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
