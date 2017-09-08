package com.alibaba.middleware.race.sync;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by H77 on 2017/6/7.
 */
public class MysqlBinlogUtil {

    private BufferedFileDataInput bfdi;
//    private Map<String,RowMeta> schemas = new HashMap<String,RowMeta>();
    private Map<String,Row> schemas = new HashMap<String,Row>();
    private ColumnMeta columnMeta;

//    public  void parseBinlog() throws IOException, InterruptedException {
////        File file = new File("D:\\阿里中间件\\复赛\\canal_local\\canal.txt");
//        FileReader reader = new FileReader("D:\\阿里中间件\\复赛\\test\\canal.txt");
//        BufferedReader buffer = new BufferedReader(reader);
////        bfdi = new BufferedFileDataInput(file, 2*1024);
////        byte[] contents = new byte[2*1024];
////        bfdi.readFully(contents);
//        String str = null;
//        int count = 0;
//        while((str = buffer.readLine()) !=null){
//            String[] results = str.split("\\|");
//            String type = results[5];
//            int length = results.length;
//            if(count == 0){
//                int index = 6;
//                while(index <length) {
//                    String meta = results[index];
//                    String[] types = meta.split(":");
//                    columnMeta = new ColumnMeta(types[1],types[0],types[2]);
//                    index = index+3;
//                }
//                count++;
//            }
//            int index = 6;
//
//            if(type.equals("I")){
//                Row r = new Row();
//                while(index <length){
//                     String[] columnsInfo = results[index].split(":");
//                     String meta = columnsInfo[0];
//
//                     String value = results[index+2];
//                     if (columnsInfo[2].equals("1")) {
//                        r.setPrimaryValue(value);
//                     }
//                     r.getColumns().put(meta, value);
//                     index = index + 3;
//                }
//                schemas.put(r.getPrimaryValue(),r);
//            }else if(type.equals("U")){
//                String[] columnsInfo = results[index].split(":");
//                String keyName = columnsInfo[0];
//                String oldPrimaryValue = results[index+1];
//                String primaryValue = results[index+2];
//                int oldkey = Integer.parseInt(oldPrimaryValue);
//                int newkey = Integer.parseInt(primaryValue);
//                index = index + 3;
//                Row row = schemas.get(oldPrimaryValue);
//                while(index < length){
//                    String columnName = results[index].split(":")[0];
//                    String value = results[index+2];
//                    row.getColumns().put(columnName,value);
//                    index = index+3;
//                }
//                if(!oldPrimaryValue.equals(primaryValue)){
//                    schemas.remove(oldPrimaryValue);
//                    row.getColumns().put(keyName, primaryValue);
//
//                    row.setPrimaryValue(primaryValue);
//                    schemas.put(primaryValue,row);
//                }
//            }else if(type.equals("D")){
//                String primaryValue = results[index+1];
//                schemas.remove(primaryValue);
//            }
//        }
//        buffer.close();
//        reader.close();
//        System.out.println(".");
//    }
    public void mark(int readLimit) {
        bfdi.mark(readLimit);
    }
    public void read(byte[] buf) throws IOException {
        bfdi.readFully(buf);
    }
    public void read(byte[] buf, int offset, int len) throws IOException {
        bfdi.readFully(buf, offset, len);
    }

    public  static  void main(String[] args){

//        MysqlBinlogUtil util = new MysqlBinlogUtil();
//        try {
//            util.parseBinlog();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

}
