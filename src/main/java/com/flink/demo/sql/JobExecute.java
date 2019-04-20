package com.flink.demo.sql;

import com.flink.demo.join.func.IgnoreEmptyFunction;
import com.flink.demo.join.func.SplitFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;

/**
 * flink SQL 目前还不是很成熟
 *
 * @author Administrator
 */
@Deprecated
public class JobExecute {

    public static void main(String[] args) {

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStream<Tuple2<String, String>> cmdStream = env.socketTextStream("localhost", 9000)
//            //数据转换
//            .map(new SplitFunction())
//            //数据过滤
//            .filter(new IgnoreEmptyFunction());
//
//
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);
//
//        //将cmdStream转成表， 其中Tuple2中的属性f0, f1重命名为name, age
//        Table table = tableEnv.fromDataStream(cmdStream, "name,age");
//
//        Table result = tableEnv.sqlQuery("select name, age from " + table + " where age >= 20");
//
//        tableEnv.execEnv();
    }

}
