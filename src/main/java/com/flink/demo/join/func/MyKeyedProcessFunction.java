package com.flink.demo.join.func;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Administrator
 */
public class MyKeyedProcessFunction extends KeyedProcessFunction<Tuple, Tuple2<String, String>, String> {

    @Override
    public void processElement(final Tuple2<String, String> data, final Context ctx, final Collector<String> out) throws Exception {
        out.collect(data.f0 + "【" + data.f1 + "】");
    }

}
