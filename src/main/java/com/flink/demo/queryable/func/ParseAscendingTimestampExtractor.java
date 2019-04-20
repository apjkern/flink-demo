package com.flink.demo.queryable.func;

import com.flink.demo.queryable.bean.MyTuple;
import com.flink.demo.queryable.util.DataKey;
import com.flink.demo.queryable.util.FormatConstant;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.text.ParseException;

/**
 * @author Administrator
 */
public class ParseAscendingTimestampExtractor extends AscendingTimestampExtractor<MyTuple> {

    @Override
    public long extractAscendingTimestamp(final MyTuple data) {
        try {
            long msgtime = DateUtils.parseDate(data.getData().get(DataKey.TIME), new String[]{FormatConstant.DATE_FORMAT}).getTime();
            return msgtime;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }

}
