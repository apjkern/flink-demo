package com.flink.demo.queryable.func;

import com.flink.demo.queryable.bean.MyTuple;
import com.flink.demo.queryable.util.CommandType;
import com.flink.demo.queryable.util.DataKey;
import com.flink.demo.queryable.util.SUBMIT_LOGIN;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseMapFunction implements MapFunction<String, MyTuple> {

    private static final Logger LOG = LoggerFactory.getLogger(ParseMapFunction.class);
    private static final Pattern MSG_REGEX = Pattern.compile("^([^ ]+) (\\d+) ([^ ]+) ([^ ]+) \\{VID:([^,]*)(?:,([^}]+))*\\}$");

    @Override
    public MyTuple map(final String realDataString) throws Exception {
        if (StringUtils.isEmpty(realDataString)) {
            return null;
        }
        Map<String, String> outMap = parseData(realDataString);
        if (MapUtils.isEmpty(outMap)) {
            return null;
        }
        return new MyTuple(outMap.get(DataKey.VEHICLE_ID), outMap);
    }

    private Map<String, String> parseData(final String dataString) {

        final Matcher matcher = MSG_REGEX.matcher(dataString);
        if (!matcher.find()) {
            LOG.warn("无效的原始数据:{}", dataString);
            return null;
        }

        final String prefix = matcher.group(1);
        final String serialNo = matcher.group(2);
        final String vin = matcher.group(3);
        final String cmd = matcher.group(4);
        final String vid = matcher.group(5);
        final String content = matcher.group(6);

        // 只处理主动发送数据
        if (!CommandType.SUBMIT.equals(prefix)) {
            return null;
        }

        // TODO: 从黑名单模式改成白名单模式
        if (
            // 如果是补发数据直接忽略
            CommandType.SUBMIT_HISTORY.equals(cmd)
                // 过滤租赁点更新数据
                || CommandType.RENTALSTATION.equals(cmd)
                // 过滤充电站更新数据
                || CommandType.CHARGESTATION.equals(cmd)) {
            return null;
        }

        final Map<String, String> data = Maps.newHashMapWithExpectedSize(300);
        data.put(DataKey.PREFIX, prefix);
        data.put(DataKey.SERIAL_NO, serialNo);
        data.put(DataKey.VEHICLE_NUMBER, vin);
        data.put(DataKey.MESSAGE_TYPE, cmd);
        data.put(DataKey.VEHICLE_ID, vid);

        try {
            // 逗号
            int commaIndex = -1;
            do {
                // 冒号
                int colonIndex = content.indexOf((int) ':', commaIndex + 1);
                if (colonIndex == -1) {
                    break;
                }

                final String key = content.substring(commaIndex + 1, colonIndex);

                commaIndex = content.indexOf((int) ',', colonIndex + 1);
                if (commaIndex != -1) {
                    final String value = content.substring(colonIndex + 1, commaIndex);
                    data.put(key, value);
                } else {
                    final String value = content.substring(colonIndex + 1);
                    data.put(key, value);
                    break;
                }
            } while (true);
        } catch (Exception e) {
            return null;
        }

        fillTime(cmd, data);

        return data;
    }

    /**
     * 计算时间(TIME)加入data
     *  @param cmd
     * @param data*/
    public void fillTime(@NotNull final String cmd, final Map<String, String> data) {

        // TODO: 统一使用平台接收数据时间

        if (CommandType.SUBMIT_REALTIME.equals(cmd)) {
            // 如果是实时数据, 则将TIME设置为数据采集时间
            data.put(DataKey.TIME, data.get(DataKey._9999_PLATFORM_RECEIVE_TIME));
        } else if (CommandType.SUBMIT_LOGIN.equals(cmd)) {
            // 由于网络不稳定导致断线重连的情况, 这类报文歧义不小.

            // 如果是注册报文, 则将TIME设置为登入时间或者登出时间或者注册时间
            if (data.containsKey(SUBMIT_LOGIN.LOGIN_TIME)) {
                // 将TIME设置为登入时间
                data.put(DataKey.TIME, data.get(SUBMIT_LOGIN.LOGIN_TIME));
            } else if (data.containsKey(SUBMIT_LOGIN.LOGOUT_TIME)) {
                // 将TIME设置为登出时间
                data.put(DataKey.TIME, data.get(SUBMIT_LOGIN.LOGOUT_TIME));
            } else {
                // 将TIME设置为注册时间
                data.put(DataKey.TIME, data.get(SUBMIT_LOGIN.REGIST_TIME));
            }
        } else if (CommandType.SUBMIT_TERMSTATUS.equals(cmd)) {
            // 如果是状态信息上报, 则将TIME设置为采集时间(地标)
            data.put(DataKey.TIME, data.get(DataKey._3101_COLLECT_TIME));
        } else if (CommandType.SUBMIT_CARSTATUS.equals(cmd)) {
            // 车辆运行状态, 采集时间
            data.put(DataKey.TIME, data.get("3201"));
        }

        if (data.get(DataKey.TIME) == null) {
            data.remove(DataKey.TIME);
        }
    }

}
