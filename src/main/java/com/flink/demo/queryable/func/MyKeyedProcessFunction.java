package com.flink.demo.queryable.func;

import com.alibaba.fastjson.JSON;
import com.flink.demo.queryable.bean.MyTuple;
import com.flink.demo.queryable.util.DataKey;
import com.flink.demo.queryable.util.FormatConstant;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author Administrator
 */
public class MyKeyedProcessFunction extends KeyedProcessFunction<Tuple, MyTuple, String> {
    private static final Logger LOG = LoggerFactory.getLogger(MyKeyedProcessFunction.class);
    private static final String NOTICE_STATUS = "status";
    private static final String NOTICE_LEVEL = "level";
    private String name;
    private int count;
    private MapState<String, String> state;
    private String stateName;

    public MyKeyedProcessFunction(final String name, String sateName) {
        this.name = name;
        this.stateName = sateName;
    }

    @Override
    public void open(final Configuration parameters) throws Exception {
        MapStateDescriptor descriptor =
            new MapStateDescriptor<>(
                "cache",
                TypeInformation.of(new TypeHint<String>() {
                }),
                TypeInformation.of(new TypeHint<String>() {
                }));
        //一个状态名称只能对应一个操作，所以通过构造函数传进来
        descriptor.setQueryable(this.stateName);
        this.state = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(final MyTuple data, final Context ctx, final Collector<String> out) throws Exception {
        final String vid = data.getData().get(DataKey.VEHICLE_ID);
        final String codeValues = data.getData().get(DataKey._2922);
        if (StringUtils.isEmpty(codeValues)) {
            LOG.info("VID:{} 故障码为空, 忽略处理.", vid);
            return;
        }

        if (!codeValues.equals("1111")) {
            return;
        }
        String notice = generateNotice(vid, data.getData());
        out.collect(notice);
        state.put(vid, notice);
    }

    private String generateNotice(String vid, Map<String, String> data) {
        Map<String, Object> notice = new HashMap<String, Object>();
        String noticeTime = DateFormatUtils.format(System.currentTimeMillis(), FormatConstant.DATE_FORMAT);

        String msgId = UUID.randomUUID().toString();
        String msgType = "FAULT_CODE_ALARM";

        notice = new HashMap<>();
        notice.put("msgType", msgType);
        notice.put("msgId", msgId);
        notice.put("vid", vid);

        notice.put(NOTICE_STATUS, 1);
        notice.put("stime", data.get(DataKey.TIME));
        notice.put("slocation", "100,100");

        notice.put("ruleId", "aaa");
        notice.put("faultCode", "1111");
        notice.put("noticetime", noticeTime);
        notice.put("faultId", "a1111");
        notice.put("analyzeType", "1");

        count += 1;
        return this.name + "(" + this.count + ") --> " + JSON.toJSONString(notice);

    }

}
