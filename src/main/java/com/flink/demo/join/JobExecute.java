package com.flink.demo.join;

import com.flink.demo.join.func.MyKeyedProcessFunction;
import com.flink.demo.join.func.IgnoreEmptyFunction;
import com.flink.demo.join.func.SplitFunction;
import com.flink.demo.join.trigger.MyGlobalTrigger;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;

/**
 * 连接两条流
 *
 * @author Administrator
 */
public class JobExecute {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        try {
            //输入【20170311 zhangsan】
            DataStream<Tuple2<String, String>> stream0 = env.socketTextStream("localhost", 9000)
                //数据转换
                .map(new SplitFunction())
                //数据过滤
                .filter(new IgnoreEmptyFunction());

            //输入【20170311 good】
            DataStream<Tuple2<String, String>> stream1 = env.socketTextStream("localhost", 9001)
                //数据转换
                .map(new SplitFunction())
                //数据过滤
                .filter(new IgnoreEmptyFunction());

            //根据相同的key【20170311】连接两条流
            KeyedStream<Tuple2<String, String>, Tuple> keyedStream = stream0.join(stream1)
                .where(source -> source.f0)
                .equalTo(target -> target.f0)
                .window(GlobalWindows.create())
                .trigger(new MyGlobalTrigger())
                .apply(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> join(final Tuple2<String, String> first, final Tuple2<String, String> second) throws Exception {
                        return Tuple2.of(first.f0, first.f1 + " " + second.f1);
                    }
                })
                .keyBy(0);

            //输出连接后的内容 --> 20170311【zhangsan good】
            keyedStream.process(new MyKeyedProcessFunction()).print();

            env.execute("my job");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
