package com.alibaba.middleware.race.sync;

/**
 * Created by mac on 17/6/24.
 */
public class FilterObject {


    private int upper;

    private int low;

    private int interval ;

    private byte[] score;

    private byte[] name;

    public byte[] getScore() {
        return score;
    }

    public void setScore(byte[] score) {
        this.score = score;
    }

    public byte[] getName() {
        return name;
    }

    public void setName(byte[] name) {
        this.name = name;
    }

    public FilterObject(int upper, int low, int interval) {
        this.upper = upper;
        this.low = low;
        this.interval = interval;
    }

    public FilterObject(int upper, int low, int interval, byte[] score, byte[] name) {
        this.upper = upper;
        this.low = low;
        this.interval = interval;
        this.score = score;
        this.name = name;
    }

    public int getUpper() {
        return upper;
    }

    public void setUpper(int upper) {
        this.upper = upper;
    }

    public int getLow() {
        return low;
    }

    public void setLow(int low) {
        this.low = low;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }
}
