package com.kafka.monitor.common;

import com.kafka.monitor.common.model.TopicPartitionOffsetModel;
import kafka.api.*;
import kafka.client.ClientUtils;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.consumer.SimpleConsumer;
import kafka.javaapi.OffsetResponse;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Map;
import scala.collection.mutable.Buffer;

import java.util.*;

/**
 * Created by lixun on 2017/3/23.
 */
public class GetOffsetShellCommand {

    private static String  clientId = "GetOffsetShell";
    private static String  brokerList = "ip:9092";
//    private static String  brokerList = "node1:9092";

    private static int   time = -1;
    private static int  maxWaitMs = 1000;
    private static int  nOffsets = 1;


    public static void main(String[] args) {
        String  topic = "logs_app";
        List<TopicPartitionOffsetModel> topicPartitionOffset = getTopicPartitionOffset(topic);
        System.out.println(topicPartitionOffset);
    }

    public static List<TopicPartitionOffsetModel> getTopicPartitionOffset(String topic) {
        List<TopicPartitionOffsetModel> list = new ArrayList<TopicPartitionOffsetModel>();

        Seq<BrokerEndPoint> metadataTargetBrokers = ClientUtils.parseBrokerList(brokerList);
        Set<String> topics = new HashSet<String>();
        topics.add(topic);
        scala.collection.mutable.Set<String> topicsSet = JavaConverters.asScalaSetConverter(topics).asScala();

        Seq<TopicMetadata> topicsMetadata = ClientUtils.fetchTopicMetadata(topicsSet, metadataTargetBrokers, clientId, maxWaitMs, 0).topicsMetadata();
        Buffer<TopicMetadata> topicMetadataList = topicsMetadata.toBuffer();
        List<TopicMetadata> topicMetadatas = JavaConverters.bufferAsJavaListConverter(topicMetadataList).asJava();

        if(topicMetadatas.size() != 1 || !topicMetadatas.get(0).topic().equals(topic)) {
            System.err.println("Error: no valid topic metadata for topic: "+topic+", probably the topic does not exist, run kafka-list-topic.sh to verify");
            System.exit(1);
        }

        TopicMetadata head = topicsMetadata.head();
        Buffer<PartitionMetadata> partitionMetadataBuffer = head.partitionsMetadata().toBuffer();
        List<PartitionMetadata> partitionMetadatas = JavaConverters.bufferAsJavaListConverter(partitionMetadataBuffer).asJava();
        List<Integer> partitions = new ArrayList<Integer>();
        for (PartitionMetadata pm : partitionMetadatas) {
            BrokerEndPoint leader = pm.leader().get();
            int port = leader.port();
            String host = leader.host();
            int partitionId = pm.partitionId();
            kafka.javaapi.consumer.SimpleConsumer consumer = new kafka.javaapi.consumer.SimpleConsumer(host, port, 10000, 100000, clientId);
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionId);
            PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(time, nOffsets);
            java.util.Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
            requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(time, nOffsets));
            kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientId);
            kafka.javaapi.OffsetResponse response = consumer.getOffsetsBefore(request);
            long[] offsets = response.offsets(topic, partitionId);
            TopicPartitionOffsetModel model = new TopicPartitionOffsetModel(topic, partitionId, offsets[0]);
            list.add(model);
        }
        return list;
    }
}
