package com.alibaba.middleware.race.sync;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by H77 on 2017/6/7.
 */
public class Row {

//    String primaryValue;

//    long primary;

//    Map<String,String> columns;

//    Map<MyKey,byte[]> columnBytes;


    byte[][] columns = new byte[6][];

//    int num = 0;

    public Row() {
    }

//    public Map<MyKey, byte[]> getColumnBytes() {
//        return columnBytes;
//    }
//
//    public void setColumnBytes(Map<MyKey, byte[]> columnBytes) {
//        this.columnBytes = columnBytes;
//    }

    public byte[][] getColumns() {
        return columns;
    }

    public void setColumns(byte[][] columns) {
        this.columns = columns;
    }

//    public long getPrimary() {
//        return primary;
//    }
//
//    public void setPrimary(long primary) {
//        this.primary = primary;
//    }


}
