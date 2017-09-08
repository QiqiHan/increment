package com.alibaba.middleware.race.sync;

/**
 * Created by H77 on 2017/6/7.
 */
public class ColumnMeta {

    private String type;
    private String name;
    private String primary;
    private MyKey myKey;



    public MyKey getMyKey() {
        return myKey;
    }

    public void setMyKey(MyKey myKey) {
        this.myKey = myKey;
    }


    public ColumnMeta(String type, String name, String primary) {
        this.type = type;
        this.name = name;
        this.primary = primary;
    }

    public ColumnMeta(MyKey myKey) {
        this.myKey = myKey;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPrimary() {
        return primary;
    }

    public void setPrimary(String primary) {
        primary = primary;
    }
}
