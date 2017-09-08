package com.alibaba.middleware.race.sync;

/**
 * Created by mac on 17/6/21.
 */
public class WriteObject {

    private byte[] bytes;

    private int index;

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
