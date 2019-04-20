package com.flink.demo.queryable;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.util.concurrent.CompletableFuture;

/**
 * 状态查询依赖 flink-queryable-state-runtime
 *
 * @author Administrator
 */
public class JobQuery {
    private static final int BOOTSTRAP_RETRIES = 240;
    private static final int numIterations = 1500;

    /**
     * 报文车辆VID
     */
    private static final String key = "69815928-1580-46a8-a27a-fc3178eba099";

    /**
     * 控制台日志提取
     * org.apache.flink.runtime.dispatcher.StandaloneDispatcher - Submitting job 29d72c59af9f4dce7d52e4d66714a8a8 (my job).
     */
    private static final String jobId = "29d72c59af9f4dce7d52e4d66714a8a8";

    public static void main(String[] args) throws Exception {

        QueryableStateClient client = new QueryableStateClient("localhost", 9069);
        //这个配置很鸡贼，不设置的话会报错，不知道为会没有默认Config
        client.setExecutionConfig(new ExecutionConfig());

        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>(
            "cache",
            TypeInformation.of(new TypeHint<String>() {
            }),
            TypeInformation.of(new TypeHint<String>() {
            }));

        // query state
        for (int iterations = 0; iterations < numIterations; iterations++) {
            CompletableFuture<MapState<String, String>> resultFuture =
                client.getKvState(
                    JobID.fromHexString(jobId),
                    "IGNITE_SHUT_MESSAGE",
                    key,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    stateDescriptor);

            resultFuture.thenAccept(response -> {
                try {
                    String tuple = response.get(key);
                    System.out.println(key + " --> " + tuple);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            Thread.sleep(100L);
        }

    }

}
