package com.flink.demo.join.trigger;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 转为全局窗口默认都是 CONTINUE 所以要自己去定义什么时候做什么事情，这个例子的目的是为了在窗口连接的时候将两个结果拼在一起， 所以只要实现onElement就行了
 * <p>
 * TriggerResult参数意思如下：
 * CONTINUE - 什么也不做
 * FIRE - 触发计算
 * PURGE - 清除窗口数据
 * FIRE_AND_PURGE - 触发计算并清除窗口数据
 *
 * @author Administrator
 */
public class MyGlobalTrigger extends Trigger<CoGroupedStreams.TaggedUnion<Tuple2<String, String>, Tuple2<String, String>>, GlobalWindow> {
    @Override
    public TriggerResult onElement(final CoGroupedStreams.TaggedUnion<Tuple2<String, String>, Tuple2<String, String>> element, final long timestamp, final GlobalWindow window, final TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onProcessingTime(final long time, final GlobalWindow window, final TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public TriggerResult onEventTime(final long time, final GlobalWindow window, final TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public void clear(final GlobalWindow window, final TriggerContext ctx) throws Exception {
    }
}
