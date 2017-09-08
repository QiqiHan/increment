package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by guojianpeng on 2017/6/8.
 */
public class NormalWriter {
    Logger logger = LoggerFactory.getLogger(Client.class);
    private FileWriter fw = null;

    private Map<Long,String> rows = new HashMap<>();

    private long begin ;

    private long end;

    public long getBegin() {
        return begin;
    }

    public void setBegin(long begin) {
        this.begin = begin;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public void add(Long key , String body ){
         rows.put(key,body);
    }


    public NormalWriter(String s) {
        try {
            fw = new FileWriter(s);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeLine(String s){
        try {
            fw.write(s+"\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeLine(){
        try {
            String str;
            for (long i = begin + 1; i < end; i++) {
                str = rows.get(i);
                if (str != null) {
                    fw.write(str + "\n");
                }
            }
            fw.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public void closeWriter(){
        try {
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
