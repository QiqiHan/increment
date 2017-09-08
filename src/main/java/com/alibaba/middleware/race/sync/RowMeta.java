package com.alibaba.middleware.race.sync;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by H77 on 2017/6/7.
 */
public class RowMeta {
    Map<String,ColumnMeta> columnsMeta;

    public RowMeta(){
        columnsMeta = new HashMap<>();
    }
}
