package com.kafka.monitor.common.model;

import com.google.gson.Gson;

/**
 * Created by lixun on 2017/3/21.
 */
public class TupleModel {
    private long timespan;
    private String ret;
    private boolean status;

    public TupleModel() {
        this.ret = "";
    }

    public boolean isStatus() {
        return this.status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public long getTimespan() {
        return this.timespan;
    }

    public void setTimespan(long timespan) {
        this.timespan = timespan;
    }

    public String getRet() {
        return this.ret;
    }

    public void setRet(String ret) {
        this.ret = ret;
    }

    public String toString() {
        return new Gson().toJson(this);
    }
}
