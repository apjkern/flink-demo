package com.flink.demo.helloword;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * hello
 *
 * @author Administrator
 */
public class JobExecute {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        try {
            env.socketTextStream("localhost", 9000)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(final String row, final Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] rows = row.split(" ");
                        for (String item : rows) {
                            if (StringUtils.isEmpty(item)) {
                                continue;
                            }
                            collector.collect(Tuple2.of(item, 1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1)
                .print();

            env.execute("my job");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
