package com.kafka.monitor.common.client;

import com.kafka.monitor.common.GetOffsetShellCommand;
import com.kafka.monitor.common.model.TopicPartitionOffsetModel;

import java.util.List;

/**
 * Created by lixun on 2017/3/28.
 */
public class GetTopicOffset {
    public static void main(String[] args) {
        String  topic = "logs_app";
        List<TopicPartitionOffsetModel> topicPartitionOffset = GetOffsetShellCommand.getTopicPartitionOffset(topic);
        long offset = 0;
        for (TopicPartitionOffsetModel model : topicPartitionOffset) {
            offset += model.getOffset();
        }
        System.out.println("offset="+offset);
    }
}
