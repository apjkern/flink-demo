package com.flink.demo.queryable.bean;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Administrator
 */
public class MyTuple implements Serializable {

    private String vid;
    private Map<String, String> data;

    public MyTuple() {
    }

    public MyTuple(final String vid, final Map<String, String> data) {
        this.vid = vid;
        this.data = data;
    }

    public String getVid() {
        return vid;
    }

    public void setVid(final String vid) {
        this.vid = vid;
    }

    public Map<String, String> getData() {
        return data;
    }

    public void setData(final Map<String, String> data) {
        this.data = data;
    }
}
