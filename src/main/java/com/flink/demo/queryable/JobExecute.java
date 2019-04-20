package com.flink.demo.queryable;

import com.flink.demo.queryable.bean.MyTuple;
import com.flink.demo.queryable.func.MyKeyedProcessFunction;
import com.flink.demo.queryable.func.ParseAscendingTimestampExtractor;
import com.flink.demo.queryable.func.ParseFilterFunction;
import com.flink.demo.queryable.func.ParseMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * flink状态查询
 * 状态查询需要注意的是每个操作的stateName不能相同
 *
 * @author Administrator
 */
public class JobExecute {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend());
        env.enableCheckpointing(1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(0);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        try {
            KeyedStream<MyTuple, Tuple> keyedStream = env.socketTextStream("localhost", 9000)
                //数据转换
                .map(new ParseMapFunction())
                //数据过滤
                .filter(new ParseFilterFunction())
                //添加时间水印
                .assignTimestampsAndWatermarks(new ParseAscendingTimestampExtractor())
                //按vid分组
                .keyBy("vid");

            //生成【故障码】通知信息
            keyedStream.process(new MyKeyedProcessFunction("故障码", "FAULT_ALARM_CODE"))
                //将通知输出到控制台
                .print();

            //生成【点火熄火最大车速】通知信息
            keyedStream.process(new MyKeyedProcessFunction("点火熄火最大车速", "IGNITE_SHUT_MESSAGE"))
                //将通知输出到控制台
                .print();

            //生成【SOC过低】通知信息
            keyedStream.process(new MyKeyedProcessFunction("SOC过低", "SOC_ALARM"))
                //将通知输出到控制台
                .print();

            env.execute("my job");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
