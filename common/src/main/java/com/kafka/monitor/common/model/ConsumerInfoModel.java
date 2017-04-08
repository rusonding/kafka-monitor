package com.kafka.monitor.common.model;

import com.google.gson.Gson;
/**
 * Created by lixun on 2017/3/22.
 */
public class ConsumerInfoModel {
    private String group;
    private String topic;
    private long logSize;
    private long offsets;
    private long lag;
    private String created;
    private long startingOffset;


    public ConsumerInfoModel(String group, String topic, long offsets, String created) {
        this.group = group;
        this.topic = topic;
        this.offsets = offsets;
        this.created = created;
    }

    public ConsumerInfoModel(String group, String topic, long logSize, long offsets, long lag, String created) {
        this.group = group;
        this.topic = topic;
        this.logSize = logSize;
        this.offsets = offsets;
        this.lag = lag;
        this.created = created;
    }

    public ConsumerInfoModel() {
        this.group = "";
        this.topic = "";
        this.logSize = 0L;
        this.offsets = 0L;
        this.lag = 0L;
        this.created = "";
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    public void setStartingOffset(long startingOffset) {
        this.startingOffset = startingOffset;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getLogSize() {
        return logSize;
    }

    public void setLogSize(long logSize) {
        this.logSize = logSize;
    }

    public long getOffsets() {
        return offsets;
    }

    public void setOffsets(long offsets) {
        this.offsets = offsets;
    }

    public long getLag() {
        return lag;
    }

    public void setLag(long lag) {
        this.lag = lag;
    }

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
