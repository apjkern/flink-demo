package com.flink.demo.queryable.func;

import com.flink.demo.queryable.bean.MyTuple;
import com.flink.demo.queryable.util.DataKey;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @author Administrator
 */
public class ParseFilterFunction implements FilterFunction<MyTuple> {

    @Override
    public boolean filter(final MyTuple tuple) throws Exception {
        // 过滤为空的数据
        if ( tuple == null || MapUtils.isEmpty(tuple.getData())) {
            return false;
        }
        // 过滤时间不对的数据
        if( !tuple.getData().containsKey(DataKey.TIME) ){
            //时间key不存在
            return false;
        }
        return true;
    }

}
