package com.alibaba.middleware.race.sync;

import java.util.Arrays;

/**
 * Created by mac on 17/6/12.
 */
public class MyKey {

    private byte[] bytes;
    public MyKey(byte[] bytes ) {
        this.bytes = bytes;
    }


    public MyKey(byte[] bytes , int begin , int end) {
        this.bytes = Arrays.copyOfRange(bytes,begin,end);
    }

    @Override
    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//
        MyKey myKey = (MyKey) o;

        return bytes[1] == myKey.bytes[1];
    }

    @Override
    public int hashCode() {
//        return Arrays.hashCode(new byte[]{bytes[0],bytes[1]});
        return bytes[1];
    }
}
