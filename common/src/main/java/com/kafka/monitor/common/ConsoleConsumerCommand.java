package com.kafka.monitor.common;


import com.kafka.monitor.common.model.KafkaPartitionModel;
import com.kafka.monitor.common.util.CalendarUtils;
import com.kafka.monitor.common.util.GroupHashCode;
import kafka.api.*;
import kafka.client.ClientUtils;
import kafka.cluster.BrokerEndPoint;
import kafka.common.MessageFormatter;
import kafka.common.OffsetAndMetadata;
import kafka.common.TopicAndPartition;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.SimpleConsumer;
import kafka.coordinator.GroupMetadataManager;
import kafka.coordinator.GroupTopicPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.Buffer;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by lixun on 2017/3/21.
 */
public class ConsoleConsumerCommand {
    private static Logger LOG = LoggerFactory.getLogger(ConsoleConsumerCommand.class);

    public static int useLeaderReplica = -1;
    public static String brokerList = "172.17.2.25:9092,172.17.2.34:9092";//SystemConfigUtils.getProperty("kafka.broker.list");
    private static String topic = "__consumer_offsets";
    private static int replicaId = useLeaderReplica;
    private static int fetchSize =1024 * 1024 * 1024;
    private static String clientId = "SimpleConsumerShell";
    private static int maxWaitMs = 1000;
    private static int maxMessages = 6000;//Integer.MAX_VALUE;
    private static boolean skipMessageOnError = false;
    private static boolean printOffsets = false;
    private static boolean noWaitAtEndOfLog = true;

    public static void main(String[] args) throws Exception {
        long startingOffset = OffsetRequest.EarliestTime();
        String group = "bi_useractionlogs_app_flume";
//        String group = "testConsumer4";
        int partitionId = GroupHashCode.getPartition(group);
        startingOffset = -2;
        Map<String, KafkaPartitionModel> result = getConsumerGroupOffset(group, startingOffset);
        java.util.Iterator<Map.Entry<String, KafkaPartitionModel>> iterator = result.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, KafkaPartitionModel> map = iterator.next();
            String topicGroup = map.getKey();
            KafkaPartitionModel value = map.getValue();
            System.out.println("topicGroup="+topicGroup+", value="+value);
        }
    }

    public static Map<String, KafkaPartitionModel> getConsumerGroupOffset(String consumer, long startingOffset) throws Exception {

        int partitionId = GroupHashCode.getPartition(consumer);
//        Map<Long, List<KafkaPartitionModel>> result = new HashMap<Long, List<KafkaPartitionModel>>();
        Map<String, KafkaPartitionModel> result = new HashMap<String, KafkaPartitionModel>();

        Class<?> messageFormatterClass = null;
        try {
            messageFormatterClass = Class.forName("kafka.coordinator.GroupMetadataManager$OffsetsMessageFormatter");
        } catch (ClassNotFoundException e) {
            LOG.error("GroupMetadataManager$OffsetsMessageFormatter not found" ,e );
            e.printStackTrace();
        }
        FetchRequestBuilder fetchRequestBuilder = new FetchRequestBuilder()
                .clientId(clientId)
                .replicaId(Request.DebuggingConsumerId())
                .maxWait(maxWaitMs)
                .minBytes(ConsumerConfig.MinFetchBytes());

        Seq<BrokerEndPoint> metadataTargetBrokers = ClientUtils.parseBrokerList(brokerList);
        Set<String> topics = new HashSet<String>();
        topics.add(topic);
        scala.collection.mutable.Set<String> topicsSet = JavaConverters.asScalaSetConverter(topics).asScala();

        //fetch learder every partition
        Seq<TopicMetadata> topicsMetadata = null;
        try {
            topicsMetadata = ClientUtils.fetchTopicMetadata(topicsSet, metadataTargetBrokers, clientId, maxWaitMs, 0).topicsMetadata();
        } catch (Exception e) {
            LOG.error("fetch learder by consmuer[" + consumer + "],startingOffset[" + startingOffset + "] error: ", e);
        }
//        scala.collection.immutable.List<TopicMetadata> topicMetadataList = topicsMetadata.toList();
        Buffer<TopicMetadata> topicMetadataList = topicsMetadata.toBuffer();

//        List<TopicMetadata> topicMetadatas = JavaConverters.asJavaListConverter(topicMetadataList).asJava();
        List<TopicMetadata> topicMetadatas = JavaConverters.bufferAsJavaListConverter(topicMetadataList).asJava();

        if(topicMetadatas.size() != 1 || !topicMetadatas.get(0).topic().equals(topic)) {
            System.err.println("Error: no valid topic metadata for topic: "+topic+", what we get from server is only:"+topicsMetadata);
            System.exit(1);
        }
//        Seq<PartitionMetadata> partitionsMetadata = topicMetadatas.get(0).partitionsMetadata();
        Buffer<PartitionMetadata> partitionsMetadata = topicMetadatas.get(0).partitionsMetadata().toBuffer();
//        List<PartitionMetadata> partitionMetadatas = JavaConverters.asJavaListConverter(partitionsMetadata).asJava();
        List<PartitionMetadata> partitionMetadatas = JavaConverters.bufferAsJavaListConverter(partitionsMetadata).asJava();

        PartitionMetadata partitionMetadataOpt = null;
        for (PartitionMetadata p : partitionMetadatas) {
            if (p.partitionId() == partitionId) {
                partitionMetadataOpt = p;
                break;
            }
        }
        if (partitionMetadataOpt == null) {
            System.err.println("Error: partition "+partitionId+" does not exist for topic "+topic);
            System.exit(1);
        }
        // validating replica id and initializing target broker
        BrokerEndPoint fetchTargetBroker = null;
        Option<BrokerEndPoint> replicaOpt = null;
        if (replicaId == useLeaderReplica) {
            replicaOpt = partitionMetadataOpt.leader();
            if (!replicaOpt.isDefined()) {
                System.err.println("Error: user specifies to fetch from leader for partition ("+topic+","+partitionId+") which has not been elected yet");
                System.exit(1);
            }
            fetchTargetBroker = replicaOpt.get();
        } else {
//            Seq<BrokerEndPoint> replicas = partitionMetadataOpt.replicas();
            Buffer<BrokerEndPoint> replicas = partitionMetadataOpt.replicas().toBuffer();
//            List<BrokerEndPoint> replicasForPartitions = JavaConverters.asJavaListConverter(replicas).asJava();
            List<BrokerEndPoint> replicasForPartitions = JavaConverters.bufferAsJavaListConverter(replicas).asJava();
            for (BrokerEndPoint bep : replicasForPartitions) {
                if (bep.id() == replicaId) {
                    fetchTargetBroker = bep;
                }
            }
            if(fetchTargetBroker == null) {
                System.err.println("Error: replica "+replicaId+" does not exist for partition ("+topic+", "+partitionId+")");
                System.exit(1);
            }
        }
        // initializing starting offset
        if(startingOffset < OffsetRequest.EarliestTime()) {
            System.err.println("Invalid starting offset: " + startingOffset);
            System.exit(1);
        }
        if (startingOffset < 0) {
            SimpleConsumer simpleConsumer = new SimpleConsumer(fetchTargetBroker.host(),
                    fetchTargetBroker.port(),
                    ConsumerConfig.SocketTimeout(),
                    ConsumerConfig.SocketBufferSize(), clientId);
            try {
                startingOffset = simpleConsumer.earliestOrLatestOffset(new TopicAndPartition(topic, partitionId), startingOffset, Request.DebuggingConsumerId());
            } catch (Exception e) {
                System.err.println("Error in getting earliest or latest offset due to: " + Utils.stackTrace(e));
                System.exit(1);
            } finally {
                if (simpleConsumer != null)
                    simpleConsumer.close();
            }
        }

        // initializing formatter
        MessageFormatter formatter = (MessageFormatter)messageFormatterClass.newInstance();
//        formatter.init(formatterArgs)
        String replicaString = "";
        if(replicaId > 0) {
            replicaString = "leader";
        } else {
            replicaString = "replica";
        }
        System.out.println("Starting simple consumer shell to partition ["+topic+", "+partitionId+"], "+replicaString+" ["+replicaId+"], host and port: ["+fetchTargetBroker.host()+", "+fetchTargetBroker.port()+"], from offset ["+startingOffset+"]");
        SimpleConsumer simpleConsumer = new SimpleConsumer(fetchTargetBroker.host(),
                fetchTargetBroker.port(),
                10000, 64*1024, clientId);

        long offset = startingOffset;
        int numMessagesConsumed = 0;
//        List<KafkaPartitionModel> models = new ArrayList<KafkaPartitionModel>();
        try {
            while (numMessagesConsumed < maxMessages) {
                FetchRequest fetchRequest = fetchRequestBuilder.addFetch(topic, partitionId, offset, fetchSize).build();
                FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);

                ByteBufferMessageSet messageSet = fetchResponse.messageSet(topic, partitionId);
                if (messageSet.validBytes() <= 0 && noWaitAtEndOfLog) {
                    startingOffset = offset;
                    System.out.println("Terminating. Reached the end of partition ("+topic+", "+partitionId+") at offset "+offset);
//                    result.put(startingOffset, models);
                    return result;
                }
                System.out.println("multi fetched " + messageSet.sizeInBytes() + " bytes from consumer "+consumer+" offset " + offset+", startingOffset: "+startingOffset);
                Iterator<MessageAndOffset> iterator = messageSet.iterator();

                while (iterator.hasNext() && numMessagesConsumed < maxMessages) {
                    MessageAndOffset messageAndOffset = iterator.next();
                    try {
                        offset = messageAndOffset.nextOffset();
                        Message message = messageAndOffset.message();
                        if (printOffsets) {
                            System.out.println("next offset = " + offset);
                        }
                        byte[] key = message.hasKey() ? Utils.readBytes(message.key()) : null;
                        byte[] value = message.isNull() ? null : Utils.readBytes(message.payload());
                        int serializedKeySize = message.hasKey() ? key.length : -1;
                        int serializedValueSize = (message.isNull()) ? -1 : value.length;
                        ConsumerRecord record = new ConsumerRecord(topic, partitionId, offset, message.timestamp(), message.timestampType(), message.checksum(), serializedKeySize, serializedValueSize, key, value);

                        KafkaPartitionModel model = parsePartitionInfo(record);
                        //kafka默认最晚读取的offset越大，所以只保存最大的offset到map中
                        if (model != null) {
                            String resultKey = model.getConsumerGroup()+","+model.getTopic()+","+model.getConsumerPartitionId();
                            model.setStartingOffset(startingOffset);
                            result.put(resultKey, model);
//                            models.add(model);
                        }
                        numMessagesConsumed += 1;
                    } catch (Exception e) {
                        if (skipMessageOnError)
                            System.err.println("Error processing message, skipping this message: "+ e.getMessage());
                        else
                            e.printStackTrace();
                    }
                    if (System.out.checkError()) {
                        // This means no one is listening to our output stream any more, time to shutdown
                        System.err.println("Unable to write to standard out, closing consumer.");
                        formatter.close();
                        simpleConsumer.close();
                        System.exit(1);
                    }
                }
                startingOffset = offset;
            }
        } catch (Exception e) {
            System.err.println("Error consuming topic, partition, replica (%s, %s, %s) with offset [%s]".format(topic+"", partitionId+"", replicaId+"", offset+""));
            e.printStackTrace();
        } finally {
            System.out.println("Consumed "+numMessagesConsumed+" messages");
        }
//        result.put(startingOffset, models);
        return result;
    }

    private static KafkaPartitionModel parsePartitionInfo(ConsumerRecord consumerRecord) {
        KafkaPartitionModel model = null;
//        String topic = consumerRecord.topic();
        long offset = consumerRecord.offset();
        int partition = consumerRecord.partition();
        String createTime = CalendarUtils.timeSpan2StrDate(consumerRecord.timestamp());
        GroupTopicPartition groupTopicPartition = null;
        Object groupTopicPartitionObj = GroupMetadataManager.readMessageKey(ByteBuffer.wrap((byte[]) consumerRecord.key())).key();

        if (groupTopicPartitionObj instanceof GroupTopicPartition) {
            groupTopicPartition = (GroupTopicPartition)groupTopicPartitionObj;
        }
        if (groupTopicPartition != null && consumerRecord.value() != null) {
            OffsetAndMetadata offsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap((byte[]) consumerRecord.value()));
            String consumerGroup = groupTopicPartition.group();
            String topic = groupTopicPartition.topicPartition().topic();
            int consumerPartition = groupTopicPartition.topicPartition().partition();
            long commitTimestamp = offsetAndMetadata.commitTimestamp();
            long expireTimestamp = offsetAndMetadata.expireTimestamp();
            long consumerOffset = offsetAndMetadata.offset();
            model = new KafkaPartitionModel(consumerGroup, topic, consumerPartition, consumerOffset, commitTimestamp, expireTimestamp, partition, offset, createTime);
        }
        return model;
    }
}
