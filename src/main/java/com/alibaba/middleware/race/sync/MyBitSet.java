package com.alibaba.middleware.race.sync;

import java.util.BitSet;

/**
 * Created by mac on 17/6/8.
 */
public class MyBitSet {

    private BitSet usedH = new BitSet();// 高位图
    private BitSet usedl = new BitSet();// 低位图

    public int getHigh(long l) {
        // long l = 900030065410220000L;

        int j = 1000000000;

        if (l > 1000000000000000000L) {
            System.out.println("Out range!");
            return 0;
        } else {
            l = l / j; // 整除，去掉后9位
            // System.out.println((int)l);
            return (int) l;
        }

    }

    public int getLow(long l) {
        // long l = 900030065410220000L;

        int j = 1000000000;

        if (l > 1000000000000000000L) {
            System.out.println("Out range!");
            return 0;
        } else {
            l = l % j; // 取余数，去掉前9位
            // System.out.println((int)l);
            return (int) l;
        }

    }

    public void setComp(long l) {
        usedH.set(getHigh(l));
        usedl.set(getLow(l));
    }

    public boolean getComp(long l) {
        try {
            return usedH.get(getHigh(l)) && usedl.get(getLow(l));
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }


}
