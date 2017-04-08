package com.kafka.monitor.common.model;

/**
 * Created by lixun on 2017/3/23.
 */
public class TopicPartitionOffsetModel {
    private String topic;
    private int partition;
    private long offset;

    public TopicPartitionOffsetModel(String topic, int partition, long offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public TopicPartitionOffsetModel() {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "TopicPartitionOffsetModel{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                '}';
    }
}
