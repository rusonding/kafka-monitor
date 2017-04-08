package com.kafka.monitor.common.jmx;

/**
 * Created by lixun on 2017/4/5.
 */
public class KafkaRoleInfo {
    private long messagesInPerSec;
    private long bytesInPerSec;
    private long bytesOutPerSec;
    private long produceRequestCountPerSec;
    private long consumerRequestCountPerSec;
    private long flowerRequestCountPerSec;
    private long produceRequestCountTotalTime;
    private long consumerRequestCountTotalTime;
    private long flowerRequestCountTotalTime;
    private long groupCoordinatorPerSec;
    private long offsetFetchPerSec;
    private long offsetsPerSec;
    private long bytesRejectedPerSec;
    private long failedProduceRequestsPerSec;

    private int activeControllerCount;
    private int partCount;

    public long getGroupCoordinatorPerSec() {
        return groupCoordinatorPerSec;
    }

    public void setGroupCoordinatorPerSec(long groupCoordinatorPerSec) {
        this.groupCoordinatorPerSec = groupCoordinatorPerSec;
    }

    public long getOffsetFetchPerSec() {
        return offsetFetchPerSec;
    }

    public void setOffsetFetchPerSec(long offsetFetchPerSec) {
        this.offsetFetchPerSec = offsetFetchPerSec;
    }

    public long getOffsetsPerSec() {
        return offsetsPerSec;
    }

    public void setOffsetsPerSec(long offsetsPerSec) {
        this.offsetsPerSec = offsetsPerSec;
    }

    public long getBytesRejectedPerSec() {
        return bytesRejectedPerSec;
    }

    public void setBytesRejectedPerSec(long bytesRejectedPerSec) {
        this.bytesRejectedPerSec = bytesRejectedPerSec;
    }

    public long getFailedProduceRequestsPerSec() {
        return failedProduceRequestsPerSec;
    }

    public void setFailedProduceRequestsPerSec(long failedProduceRequestsPerSec) {
        this.failedProduceRequestsPerSec = failedProduceRequestsPerSec;
    }

    public long getProduceRequestCountTotalTime() {
        return produceRequestCountTotalTime;
    }

    public void setProduceRequestCountTotalTime(long produceRequestCountTotalTime) {
        this.produceRequestCountTotalTime = produceRequestCountTotalTime;
    }

    public long getConsumerRequestCountTotalTime() {
        return consumerRequestCountTotalTime;
    }

    public void setConsumerRequestCountTotalTime(long consumerRequestCountTotalTime) {
        this.consumerRequestCountTotalTime = consumerRequestCountTotalTime;
    }

    public long getFlowerRequestCountTotalTime() {
        return flowerRequestCountTotalTime;
    }

    public void setFlowerRequestCountTotalTime(long flowerRequestCountTotalTime) {
        this.flowerRequestCountTotalTime = flowerRequestCountTotalTime;
    }

    public long getMessagesInPerSec() {
        return messagesInPerSec;
    }

    public void setMessagesInPerSec(long messagesInPerSec) {
        this.messagesInPerSec = messagesInPerSec;
    }

    public long getBytesInPerSec() {
        return bytesInPerSec;
    }

    public void setBytesInPerSec(long bytesInPerSec) {
        this.bytesInPerSec = bytesInPerSec;
    }

    public long getBytesOutPerSec() {
        return bytesOutPerSec;
    }

    public void setBytesOutPerSec(long bytesOutPerSec) {
        this.bytesOutPerSec = bytesOutPerSec;
    }

    public long getProduceRequestCountPerSec() {
        return produceRequestCountPerSec;
    }

    public void setProduceRequestCountPerSec(long produceRequestCountPerSec) {
        this.produceRequestCountPerSec = produceRequestCountPerSec;
    }

    public long getConsumerRequestCountPerSec() {
        return consumerRequestCountPerSec;
    }

    public void setConsumerRequestCountPerSec(long consumerRequestCountPerSec) {
        this.consumerRequestCountPerSec = consumerRequestCountPerSec;
    }

    public long getFlowerRequestCountPerSec() {
        return flowerRequestCountPerSec;
    }

    public void setFlowerRequestCountPerSec(long flowerRequestCountPerSec) {
        this.flowerRequestCountPerSec = flowerRequestCountPerSec;
    }

    public int getActiveControllerCount() {
        return activeControllerCount;
    }

    public void setActiveControllerCount(int activeControllerCount) {
        this.activeControllerCount = activeControllerCount;
    }

    public int getPartCount() {
        return partCount;
    }

    public void setPartCount(int partCount) {
        this.partCount = partCount;
    }

    @Override
    public String toString() {
        return  "messagesInPerSec=" + messagesInPerSec +
                "\nbytesInPerSec=" + bytesInPerSec +
                "\nbytesOutPerSec=" + bytesOutPerSec +
                "\nproduceRequestCountPerSec=" + produceRequestCountPerSec +
                "\nconsumerRequestCountPerSec=" + consumerRequestCountPerSec +
                "\nflowerRequestCountPerSec=" + flowerRequestCountPerSec +
                "\nproduceRequestCountTotalTime=" + produceRequestCountTotalTime +
                "\nconsumerRequestCountTotalTime=" + consumerRequestCountTotalTime +
                "\nflowerRequestCountTotalTime=" + flowerRequestCountTotalTime +
                "\ngroupCoordinatorPerSec=" + groupCoordinatorPerSec +
                "\noffsetFetchPerSec=" + offsetFetchPerSec +
                "\noffsetsPerSec=" + offsetsPerSec +
                "\nbytesRejectedPerSec=" + bytesRejectedPerSec +
                "\nfailedProduceRequestsPerSec=" + failedProduceRequestsPerSec +
                "\nactiveControllerCount=" + activeControllerCount +
                "\npartCount=" + partCount;
    }
}
