package com.flink.demo.join.func;

import com.flink.demo.queryable.bean.MyTuple;
import com.flink.demo.queryable.util.DataKey;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author Administrator
 */
public class IgnoreEmptyFunction implements FilterFunction<Tuple2<String, String>> {

    @Override
    public boolean filter(final Tuple2<String, String> tuple) throws Exception {
        if(StringUtils.isEmpty(tuple.f0)){
            return false;
        }
        return true;
    }

}
