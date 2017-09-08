package com.alibaba.middleware.race.sync;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * Created by mac on 17/6/7.
 */
public class BinlogReadUtilDemo {

    private List<Long> blackList = new ArrayList<>();
    private Map<Long,Row> schemas = new HashMap<Long,Row>();
    private Queue<Row> rowsList = new LinkedList<Row>();
    private ColumnMeta columnMeta;
    private RandomAccessFile rf = null;
    private FileChannel fileChannel = null;
    private MappedByteBuffer currentContentBuffer = null;//当前buffer
    private int bufferSize = 200*1024*1024;
    private int count = 0;
    private long currentPosition = 0;
    private byte[] contents= new byte[1024*2];
    private int size = 1024;
    int contentsOffset = 0;
    int mapOffset = bufferSize-contents.length;
    private boolean first = true;
    private boolean isEnd = false;
    public BinlogReadUtilDemo(String filePath){
        try {
            rf = new RandomAccessFile("D:\\阿里中间件\\复赛\\canal_local\\canal.txt", "r");
            fileChannel = rf.getChannel();
            currentPosition = rf.length();
            map();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void map() throws Exception{
//        int n = 1;
        if(currentPosition-bufferSize >= 0){
            currentContentBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, currentPosition - bufferSize, bufferSize);
            currentPosition = currentPosition-bufferSize;
            mapOffset = bufferSize-size;
        }else{
            currentContentBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, currentPosition);
            bufferSize = (int)currentPosition;
            mapOffset = bufferSize-size;
        }
    }

    public String readLine() throws Exception {
        String result = "";
        if(contentsOffset == 0) {
            //判断下一次的mapOffset是否达到size大小，如果不到直接读完
            if((mapOffset - size) >= 0) {
                currentContentBuffer.position(mapOffset);
                currentContentBuffer.get(contents, 0, 1024);
                mapOffset = mapOffset - size;
                if(first){
                    contentsOffset = size - 2;
                    first = false;
                }else {
                    contentsOffset = size - 1;
                }
            }else{
                if(bufferSize != 200*1024*1024){
                    if(isEnd){
                        return null;
                    }
                    //bufferSize 不是200M时，说明已经进行过最后一次map了
                    currentContentBuffer.position(0);
                    currentContentBuffer.get(contents,0,size+mapOffset);
                    //这是最后一次读数组了 如果下次 进来 直接return;
                    isEnd = true;
                }else{
                    //这种情况下 最前面的map 就剩一部分了
                    currentContentBuffer.position(0);
                    currentContentBuffer.get(contents,0,size+mapOffset);
                    //重新map
                    map();
                }
                contentsOffset = size+mapOffset-1;
            }
        }
        int offset = contentsOffset;
        while( contents[offset--] != 10 ){
            if(offset == 0 ){
                 if(contents[offset] != 10 ) {
                     result = new String(contents,0,contentsOffset);
                     if( (mapOffset - size) >= 0 ) {
                         currentContentBuffer.position(mapOffset);
                         currentContentBuffer.get(contents, 0, 1024);
                         mapOffset = mapOffset - size;
                         contentsOffset = size - 1;
                     }else {
                         if(bufferSize != 200*1024*1024){
                             //bufferSize 不是200M时，就是最后一次到达上限
                             if(isEnd){
                                 contentsOffset = 0;
                                 return result;
                             }
                             currentContentBuffer.position(0);
                             currentContentBuffer.get(contents,0,size+mapOffset);
                             //这是最后一次读数组了
                             //如果下次进来 直接return;
                             isEnd = true;
                         }else {
                             //这种情况下 最前面的map 就剩一部分了
                             currentContentBuffer.position(0);
                             currentContentBuffer.get(contents,0,size+mapOffset);
                             //重新map
                             map();
                         }
                         contentsOffset = size+mapOffset-1;
                     }
                     offset = contentsOffset;
                 }else {
                     break;
                 }
            }
        };
        int realStart = offset+2;
        int length = contentsOffset - realStart+1;
        result = new String(contents,realStart,length)+result;
        contentsOffset = offset;
        return result;
    }


    public  void parseBinlog() throws IOException, InterruptedException {

        long len = rf.length();
        long start = rf.getFilePointer();
        long nextend = start + len - 1;
        rf.readLine();
        String line = "";
        try {
            line = readLine();
        } catch (Exception e) {
            e.printStackTrace();
        }
        while(line  != null){
//            dealData(line);
            try {
                line = readLine();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

//        rf.seek(nextend);
//        int c = -1;
//        while (nextend > start) {
//            c = currentContentBuffer.get();
//            if (c == '\n' || c == '\r') {
////                line = this.readLine();
//                if (line != null) {
//                    line = new String(line.getBytes("ISO-8859-1"), "UTF-8");
//                    dealData(line);
//                }
//                nextend--;
//            }
//            nextend--;
//            rf.seek(nextend);
//
//        }

//        dealData(firstline);
        System.out.println("sad");
    }

//    public void dealData(String line){
//        String[] results;
//        int index = 9;
//        results= line.split("\\|");
//        String type = results[5];
//        int length = results.length;
//        if(type.equals("I")){
//            long key =Long.parseLong(results[8]) ;
//            if(blackList.contains(key)){
//                return ;
//            }
//            if(key>=Server.upper || key<=Server.low){
//                return;
//            }
//            if(schemas.containsKey(key)){
//                Row row = schemas.get(key);
//                schemas.remove(key);
//                while(index < length){
//                    String columnName = results[index].split(":")[0];
//                    String value = results[index+2];
//                    if(!row.getColumns().containsKey(columnName)) {
//                        row.getColumns().put(columnName, value);
//                    }
//                    index = index+3;
//                }
//                rowsList.add(row);
//            }else{
//                Row row = new Row();
//                row.setPrimaryValue(results[8]);
//                index = 6;
//                while(index < length){
//                    String columnName = results[index].split(":")[0];
//                    String value = results[index+2];
//                    row.getColumns().put(columnName,value);
//                    index = index+3;
//                }
////                            schemas.put(key,row);
//                rowsList.add(row);
//            }
//        }else if(type.equals("U")){
//            long key =Long.parseLong(results[8]) ;
//            long oldkey = Long.parseLong(results[7]);
//            if(blackList.contains(key)){
//                if(key!=oldkey){
//                    blackList.add(oldkey);
//                }
//                return ;
//            }
//            if(key>=Server.upper || key<=Server.low){
//                return;
//            }
//
//            if(key == oldkey ){
//                if(schemas.containsKey(key)){
//                    Row row = schemas.get(key);
//                    while(index < length){
//                        String columnName = results[index].split(":")[0];
//                        String value = results[index+2];
//                        if(!row.getColumns().containsKey(columnName)) {
//                            row.getColumns().put(columnName, value);
//                        }
//                        index = index+3;
//                    }
//                }else{
//                    Row row = new Row();
//                    row.setPrimaryValue(results[8]);
//                    index = 6;
//                    while(index < length){
//                        String columnName = results[index].split(":")[0];
//                        String value = results[index+2];
//                        row.getColumns().put(columnName,value);
//                        index = index+3;
//                    }
//                    schemas.put(key,row);
//                }
//            }else{
//                if(schemas.containsKey(key)){
//                    Row row = schemas.get(key);
//                    schemas.remove(key);
//                    while(index < length){
//                        String columnName = results[index].split(":")[0];
//                        String value = results[index+2];
//                        if(!row.getColumns().containsKey(columnName)) {
//                            row.getColumns().put(columnName, value);
//                        }
//                        index = index+3;
//                    }
//                    schemas.put(oldkey,row);
//                }else{
//                    Row row = new Row();
//                    index = 6;
//                    row.setPrimaryValue(results[8]);
//                    while(index < length){
//                        String columnName = results[index].split(":")[0];
//                        String value = results[index+2];
//                        row.getColumns().put(columnName,value);
//                        index = index+3;
//                    }
//                    schemas.put(oldkey,row);
//                }
//            }
//
//        }else if(type.equals("D")){
//            long key =Long.parseLong(results[7]) ;
//            blackList.add(key);
//        }
//
//    }

//    public  String readLine() throws IOException {
//        StringBuilder input = new StringBuilder();
//        int c = -1;
//        boolean eol = false;
//
//        while (!eol) {
//            switch (c = currentContentBuffer.get(offset,1)) {
//                case -1:
//                case '\n':
//                    eol = true;
//                    break;
//                case '\r':
//                    eol = true;
//                    long cur = rf.getFilePointer();
//                    if ((currentContentBuffer.get()) != '\n') {
//                        seek(cur);
//                    }
//                    break;
//                default:
//                    input.append((char)c);
//                    break;
//            }
//        }
//
//        if ((c == -1) && (input.length() == 0)) {
//            return null;
//        }
//        return input.toString();
//    }

        public static void main(String[] args) throws Exception{
            BinlogReadUtilDemo binlogReadUtil = new BinlogReadUtilDemo("1");
            binlogReadUtil.parseBinlog();
    }
}
