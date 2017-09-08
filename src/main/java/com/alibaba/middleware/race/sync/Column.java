package com.alibaba.middleware.race.sync;

/**
 * Created by H77 on 2017/6/7.
 */
public class Column {
    String value;
    public Column(String value) {
        this.value = value;
    }
    public String getValue() {
        return value;
    }
    public void setValue(String value) {
        this.value = value;
    }
}
