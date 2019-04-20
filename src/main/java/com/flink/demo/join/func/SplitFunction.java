package com.flink.demo.join.func;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author Administrator
 */
public class SplitFunction implements MapFunction<String, Tuple2<String, String>> {

    private static final Tuple2<String, String> EMPTY = Tuple2.of("", "");

    /**
     * map 不允许返回null
     * @param row
     * @return
     * @throws Exception
     */
    @Override
    public Tuple2<String, String> map(final String row) throws Exception {
        if (StringUtils.isEmpty(row)) {
            return EMPTY;
        }
        String[] cols = row.split(" ");
        if (cols.length < 2) {
            return EMPTY;
        }
        return Tuple2.of(cols[0], cols[1]);
    }

}
