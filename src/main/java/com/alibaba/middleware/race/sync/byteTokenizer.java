package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Created by H77 on 2017/6/12.
 */
public class byteTokenizer {

    private static Logger logger = LoggerFactory.getLogger(Server.class);
    byte[] line;
    int begin = 0;
    int end = 1;
    byte split;
//    int length = 0;
    int[] indexes ;
    int offset = 0;
    int indexLength = 0;
//    private int index ;
    public byteTokenizer(byte[] line, byte split) {
        this.line = line;
        this.split = split;
    }
    public byte[] nextToken(){
        if (offset >= indexLength ) return null;
        begin = indexes[offset-1];
        end = indexes[offset++];

//      byte[] token = Arrays.copyOfRange(line, begin + 1, end);
        int length = end - begin - 1;
        byte[] token = new byte[length];
        System.arraycopy(line,begin+1,token,0 ,length);

        begin = end;
        return token;
    }

    public byte[] nextToken(int count){
        offset = offset+count-1;
        if(offset >= indexLength) return null;
        begin = indexes[offset-1];
        end = indexes[offset++];
//        byte[] token = Arrays.copyOfRange(line, begin + 1, end);
        int length = end - begin - 1;
        byte[] token = new byte[length];
        System.arraycopy(line,begin+1,token,0 ,length);

        begin = end;
        return token;
    }

    public int nextLength(){
        if (offset >= indexLength ) return 0;
        end = indexes[offset++];
        int length = end-begin-1;
        begin = end;
        return length;
    }

    public void setLine(byte[] line ,int[] indexes , int indexLength){
        this.line = line;
        this.indexes = indexes;
        this.indexLength = indexLength;
        this.offset = 1;
        this.begin = 0;
    }

}
