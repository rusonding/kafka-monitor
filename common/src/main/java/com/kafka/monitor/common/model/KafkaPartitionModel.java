package com.kafka.monitor.common.model;

/**
 * Created by lixun on 2017/3/21.
 */
public class KafkaPartitionModel {
    private String consumerGroup;
    private String topic;
    private int consumerPartitionId ;
    private long consumerOffset ;
    private long consumerCommitTime;
    private long consumerExpirationTime;
    private int partitionId;
    private long offset;
    private String createTime;
    private long startingOffset;

    public long getStartingOffset() {
        return startingOffset;
    }

    public void setStartingOffset(long startingOffset) {
        this.startingOffset = startingOffset;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getConsumerPartitionId() {
        return consumerPartitionId;
    }

    public void setConsumerPartitionId(int consumerPartitionId) {
        this.consumerPartitionId = consumerPartitionId;
    }

    public long getConsumerOffset() {
        return consumerOffset;
    }

    public void setConsumerOffset(long consumerOffset) {
        this.consumerOffset = consumerOffset;
    }

    public long getConsumerCommitTime() {
        return consumerCommitTime;
    }

    public void setConsumerCommitTime(long consumerCommitTime) {
        this.consumerCommitTime = consumerCommitTime;
    }

    public long getConsumerExpirationTime() {
        return consumerExpirationTime;
    }

    public void setConsumerExpirationTime(long consumerExpirationTime) {
        this.consumerExpirationTime = consumerExpirationTime;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public KafkaPartitionModel(String consumerGroup, String topic, int consumerPartitionId, long consumerOffset, long consumerCommitTime, long consumerExpirationTime, int partitionId, long offset, String createTime) {
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.consumerPartitionId = consumerPartitionId;
        this.consumerOffset = consumerOffset;
        this.consumerCommitTime = consumerCommitTime;
        this.consumerExpirationTime = consumerExpirationTime;
        this.partitionId = partitionId;
        this.offset = offset;
        this.createTime = createTime;
    }

    public KafkaPartitionModel(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    @Override
    public String toString() {
        return "KafkaPartitionModel{" +
                "consumerGroup='" + consumerGroup + '\'' +
                ", topic='" + topic + '\'' +
                ", consumerPartitionId=" + consumerPartitionId +
                ", consumerOffset=" + consumerOffset +
                ", consumerCommitTime=" + consumerCommitTime +
                ", consumerExpirationTime=" + consumerExpirationTime +
                ", partitionId=" + partitionId +
                ", offset=" + offset +
                ", createTime=" + createTime +
                '}';
    }
}
