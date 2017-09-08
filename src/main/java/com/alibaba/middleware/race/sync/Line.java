package com.alibaba.middleware.race.sync;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by H77 on 2017/6/11.
 */
public class Line {

    byte[] contents;
    int index;
    int [] indexes = new int[20];
    //    List<Integer> indexes = new ArrayList<>(17);
    int listindex = 0;//for(int i = 0 ; i<listindex ; i++)去遍历indexes



    public Line(byte[] contents, int index) {
        this.contents = contents;
        this.index = index;
    }


    public void clearLine(){
         listindex = 0;
    }

    public void addindex(int index){

        if(listindex >=indexes.length){
            int[] newindex = new int[2*indexes.length];
            System.arraycopy(this.indexes,0,newindex,0,indexes.length);
            this.indexes = newindex;
        }
        this.indexes[listindex] = index;
        listindex++;
    }

    public byte[] getContents() {
        return contents;
    }

    public void setContents(byte[] contents) {
        this.contents = contents;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int[] getIndexes() {
        return indexes;
    }

    public int getListindex() {
        return listindex;
    }
}
