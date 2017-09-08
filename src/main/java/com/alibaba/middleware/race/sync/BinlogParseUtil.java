package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by H77 on 2017/6/20.
 */
public class BinlogParseUtil {

    private static Logger logger = LoggerFactory.getLogger(Server.class);

    private Row[] rowArray;
    private int num;

    private HashMap<Integer,Row> rows;

    //缓存区
    private int contentBufferSize = 40*1024*1024;
    //private int lineBufferSize = 1024;
    private byte[] contentBuffer = new byte[contentBufferSize];

    private int readIndex = 0;

    //文件句柄
    private RandomAccessFile randomAccessFile;
    private FileChannel fc;
    private MappedByteBuffer currentContentBuffer = null;//当前buffer

    private int fileLength;
    private int position = 0;
    private boolean lastBlock = false;
    private List<FilterObject> filterObjects = new ArrayList<>();

    int contentBufferLength;


    public void setUpFilter(){
        //先过滤几个试试看
        filterObjects.add(new FilterObject(5240296,999998,2));
        filterObjects.add(new FilterObject(5181099,999999,11));
        filterObjects.add(new FilterObject(5029362,999999,13));
        filterObjects.add(new FilterObject(5051325,999990,15));
        filterObjects.add(new FilterObject(5240250,999991,17));
        filterObjects.add(new FilterObject(5233151,999989,19));
        filterObjects.add(new FilterObject(5223288,999999,21));
        filterObjects.add(new FilterObject(5143365,999999,27));
        filterObjects.add(new FilterObject(5035173,999999,33));
        filterObjects.add(new FilterObject(5036071,999990,41));
        filterObjects.add(new FilterObject(5233132,999999,37));
        filterObjects.add(new FilterObject(5163698,999965,43));
        filterObjects.add(new FilterObject(5240218,999972,47));
        filterObjects.add(new FilterObject(5088901,999951,53));
        filterObjects.add(new FilterObject(5104975,999991,59));
        filterObjects.add(new FilterObject(5120745,5000007,3));
        filterObjects.add(new FilterObject(5065430,5000005,5));
        filterObjects.add(new FilterObject(5181705,5120775,15));
        filterObjects.add(new FilterObject(5080635,5065515,45));
        filterObjects.add(new FilterObject(5051310,5043280,55));
    }

    public BinlogParseUtil(RandomAccessFile randomAccessFile , Row[] rows, HashMap<Integer,Row> rowMap ){
        this.rowArray = rows;
        this.num = rowArray.length;
        this.rows = rowMap;
        this.randomAccessFile = randomAccessFile;
        try {
            this.fc = randomAccessFile.getChannel();
            this.fileLength = Integer.parseInt(String.valueOf(randomAccessFile.length()));
            this.contentBufferLength = Math.min(contentBufferSize,
                    fileLength - position);
            this.currentContentBuffer = fc.map(FileChannel.MapMode.READ_ONLY,position, contentBufferLength);
            currentContentBuffer.get(contentBuffer,0, contentBufferLength);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public BinlogParseUtil(Row[] rows, HashMap<Integer,Row> rowMap){
        this.rowArray = rows;
        this.num = rowArray.length;
        this.rows = rowMap;
        setUpFilter();
    }

    public BinlogParseUtil(){
        setUpFilter();
    }

    public void setFile(RandomAccessFile randomAccessFile ) {
        this.randomAccessFile = randomAccessFile;
        this.fc = randomAccessFile.getChannel();
        this.position = 0;
        this.readIndex = 0;
        this.lastBlock = false;
        try {
            this.fileLength = Integer.parseInt(String.valueOf(randomAccessFile.length()));
            this.contentBufferLength = Math.min(contentBufferSize,
                    fileLength - position);
            if(contentBufferSize != contentBufferLength) this.lastBlock = true;
            this.currentContentBuffer = fc.map(FileChannel.MapMode.READ_ONLY,position, contentBufferLength);
            currentContentBuffer.get(contentBuffer,0, contentBufferLength);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setPosition(int start , int end){
        this.position = start;
        this.readIndex = 0;
        this.lastBlock = false;
        try {
            this.fileLength = end;
            this.contentBufferLength = Math.min(contentBufferSize,
                    fileLength - position);
            if(contentBufferSize != contentBufferLength) this.lastBlock = true;
            this.currentContentBuffer = fc.map(FileChannel.MapMode.READ_ONLY,position, contentBufferLength);
            currentContentBuffer.get(contentBuffer,0, contentBufferLength);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public void parseBinlog() throws IOException {

        readIndex = readIndex+54;
        //找到table后面的分隔符
        while (currentContentBuffer.get(readIndex++) != 124) ;
        byte type = currentContentBuffer.get(readIndex++);
        //根据type解析这条binlog
        if(type == 'I'){
            Row r = new Row();
            //第一位是id 跳13个字节到value开始的地方
            byte[] idArr = parseColumn(13);
            r.getColumns()[0] = idArr;
            int key = byteToInteger(idArr);
                //第二位是firstName跳20个字节到value开始的地方  读到‘ |’
                byte[] firstName = parseColumn(20);
                r.getColumns()[1] = firstName;
                //第三位是lastName跳19个字节到value开始的地方
                byte[] lastName = parseColumn(19);
                r.getColumns()[2] = lastName;
                //第四位是sex跳13个字节
                byte[] sex = parseColumn(13);
                r.getColumns()[3] = sex;
                //第五位是score跳15个字节
                byte[] score = parseColumn(15);
                r.getColumns()[4] = score;
                //第六位是score2跳16个字节
                byte[] score2 = parseColumn(16);
                r.getColumns()[5] = score2;
                if (key < num && key >= 0) {
                    rowArray[key] = r;
                } else {
                    rows.put(key, r);
                }
        } else if (type == 'U') {
                byte[] oldKeyByte = parseColumn(8);
                byte[] keyByte = parseColumn(0);
                int oldKey = byteToInteger(oldKeyByte);
                int key = byteToInteger(keyByte);
                Row row;
                if (oldKey < num && oldKey >= 0) {
                    row = rowArray[oldKey];
                } else {
                    row = rows.get(oldKey);
                }
                //解析后面的column的逻辑
                while (currentContentBuffer.get(readIndex) != '\n') {
                    updateRow(row);
                }
                if (oldKey != key) {
                    row.getColumns()[0] = keyByte;
                    if (key < num && key >= 0) {
                        rowArray[key] = row;
                    } else {
                        rows.put(key, row);
                    }
                    if (oldKey < num && oldKey >= 0) {
                        rowArray[oldKey] = null;
                    } else {
                        rows.remove(oldKey);
                    }
                }
        } else if (type == 'D') {
                //对于delete操作解析id跳7个字节
                byte[] id = parseColumn(8);
                int key = byteToInteger(id);
                if (key < num && key >= 0) {
                    rowArray[key] = null;
                } else {
                    rows.remove(key);
                }
                //跳本地65S线上78S
                readIndex = readIndex + 65;
                while (currentContentBuffer.get(readIndex) != '\n') {
                    readIndex++;
                }
        } else {
                logger.info("type走入错误分支");
        }
        //跳过换行符
        readIndex++;

            //预留350个字节 为了方便操作
            if (readIndex + 350 > contentBufferSize && !lastBlock) {
                position = position + readIndex;
                this.contentBufferLength = Math.min(contentBufferSize,
                        fileLength - position);
                if (contentBufferLength != contentBufferSize) lastBlock = true;
                this.currentContentBuffer = fc.map(FileChannel.MapMode.READ_ONLY, position, contentBufferLength);
//                currentContentBuffer.get(contentBuffer, 0, contentBufferLength);
                readIndex = 0;
            }
        }



    public boolean isParseOver(){
        if(!lastBlock){
            return true;
        }else {
            return (position + readIndex) < fileLength;
        }
    }

    private void updateRow(Row row){
        if(currentContentBuffer.get(readIndex) == 'f'){
            row.getColumns()[1] = parseColumnForUpdate(15);
        }else if(currentContentBuffer.get(readIndex) =='l'){
            row.getColumns()[2] = parseColumnForUpdate(14);
        }else if(currentContentBuffer.get(readIndex) == 's'){
            readIndex = readIndex + 7;
            if(currentContentBuffer.get(readIndex) == 124){//sex的情况
                row.getColumns()[3] = parseColumnForUpdate(1);
            }else if(currentContentBuffer.get(readIndex) == ':'){//score的情况
                row.getColumns()[4] = parseColumnForUpdate(3);
            }else if(currentContentBuffer.get(readIndex) == '1'){ //score2的情况
                row.getColumns()[5] = parseColumnForUpdate(4);
            }
        }
    }

    private byte[] parseColumn(int skip){
        readIndex = readIndex +skip;
        int tempIndex = readIndex;
        while(currentContentBuffer.get(readIndex++) != 124);
        int newLength = tempIndex - readIndex +1;
        byte[] content = new byte[newLength];
        currentContentBuffer.get(content,tempIndex,readIndex-1);
        return content;
    }
    private void onlySkipColumn(int skip){
        readIndex = readIndex +skip;
        while(currentContentBuffer.get(readIndex++) != 124);
    }
    private byte[] parseColumnForUpdate(int skip){
        readIndex = readIndex+skip;//从oldValue开始
        while (currentContentBuffer.get(readIndex++) != 124);
        int tempIndex = readIndex;
        while (currentContentBuffer.get(readIndex++) != 124);
        int newLength = tempIndex - readIndex +1;
        byte[] content = new byte[newLength];
        currentContentBuffer.get(content,tempIndex,readIndex-1);
        return content;
    }


    private byte[] copyOfRange(byte[] original, int from, int to){
        int newLength = to - from;
//        size = size +newLength;
        byte[] copy = new byte[newLength];
        System.arraycopy(original, from, copy, 0, newLength);
        return copy;
    }
    private int byteToInteger(byte[] content){
        int key = 0 ;
        int length = content.length;
        for(int i = 0 ; i < length ; i++){
            key = key*10 + (content[i]-'0');
        }
        return key;
    }


    private boolean isExist(int num){
        //先过滤几个看看效果
        for(FilterObject filterObject : filterObjects){
            if(judge(num,filterObject))
                return true;
        }
        return false;
    }

    private boolean judge(int num , FilterObject filterObject){
        if(num<=filterObject.getUpper() && num>=filterObject.getLow()){
            int j = num-filterObject.getLow();
            if((j % filterObject.getInterval()) == 0)
                return true;
        }
        return false;
    }


    public static void main(String[] args){
        int count = 0;
        BinlogParseUtil binlogParseUtil = new BinlogParseUtil();
        for(int i = 1000000; i<5240296 ; i++){
            if(!binlogParseUtil.isExist(i))
                count++;
        }
        System.out.println(count);
    }
}
