package com.kafka.monitor.common;

import com.kafka.monitor.common.model.ConsumerInfoModel;
import com.kafka.monitor.common.model.OffsetDomain;
import com.kafka.monitor.common.util.CalendarUtils;
import kafka.admin.AdminClient;
import kafka.coordinator.GroupOverview;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
//import scala.collection.immutable.HashMap;
//import scala.collection.immutable.Map;
import scala.collection.mutable.Buffer;
import kafka.admin.ConsumerGroupCommand.LogEndOffsetResult;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

/**
 * Created by lixun on 2017/3/23.
 */
public class ConsumerGroupCommand {
    public static final String KEY_TOPIC = "topic";
    public static final String KEY_PARTITION = "partition";

    public static void main(String[] args) {
        System.out.println("===========");
        String brokers = "172.17.2.25:9092,172.17.2.34:9092";

        Map<String, Object> result = describeGroup(brokers, "bi_useractionlogs_app_flume");
//        Map<String, Object> result = describeGroup(brokers, "testConsumer1");
        System.out.println(result);
    }

    private static KafkaConsumer<String, String> createNewConsumer(String brokers, String group) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
        return consumer;
    }

    public static long getLogEndOffset(KafkaConsumer<String, String> consumer, String group, String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        List<TopicPartition> partitions = new ArrayList<TopicPartition>();
        partitions.add(topicPartition);
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        long logEndOffset = consumer.position(topicPartition);

        return logEndOffset;
    }

    public static Map<String, Object> describeGroup(String brokers, String group) {
        Map<String, Object> result = new HashMap<String, Object>();
        List<ConsumerInfoModel> consumerInfoResult = new ArrayList<ConsumerInfoModel>();
        List<OffsetDomain> offsetModelResult = new ArrayList<OffsetDomain>();

        KafkaConsumer<String, String> consumer = createNewConsumer(brokers, group);
        AdminClient adminClient = createAdminClient(brokers);
        scala.collection.immutable.List<AdminClient.ConsumerSummary> consumerSummaryList = adminClient.describeConsumerGroup(group);
        Buffer<AdminClient.ConsumerSummary> buffer = consumerSummaryList.toBuffer();
        List<AdminClient.ConsumerSummary> consumerSummarys = JavaConversions.bufferAsJavaList(buffer);
        for(AdminClient.ConsumerSummary summary : consumerSummarys) {
            Buffer<TopicPartition> assignmentBuffer = summary.assignment().toBuffer();
            List<TopicPartition> topicPartitions = JavaConversions.bufferAsJavaList(assignmentBuffer);
            long logEndOffsetAll = 0l;
            long offsetAll = 0l;
            String topic = "";
            for (TopicPartition partition: topicPartitions) {
                topic = partition.topic();
                OffsetAndMetadata committed = consumer.committed(new TopicPartition(topic, partition.partition()));
                if (committed != null) {
                    long logEndOffset= getLogEndOffset(consumer, group, topic, partition.partition());

                    OffsetDomain offsetModel = new OffsetDomain();
                    offsetModel.setPartition(partition.partition());
                    offsetModel.setLogSize(logEndOffset);
                    offsetModel.setOffset(committed.offset());
                    offsetModel.setLag((committed.offset() == -1L) ? 0L : logEndOffset - committed.offset());
                    offsetModel.setOwner(group);
                    offsetModel.setCreate(CalendarUtils.getNormalDate());
                    offsetModel.setModify(CalendarUtils.getNormalDate());
                    offsetModel.setTopic(topic);
                    offsetModelResult.add(offsetModel);

                    logEndOffsetAll += logEndOffset;
                    offsetAll += (committed.offset() == -1L ? 0L : committed.offset());
                    //                System.out.println("topic="+topic+", partition="+partition.partition()+", logEndOffset="+logEndOffset+", offset="+committed.offset());
                }
            }
            ConsumerInfoModel model = new ConsumerInfoModel();
            model.setTopic(topic);
            model.setGroup(group);
            model.setLogSize(logEndOffsetAll);
            model.setOffsets(offsetAll);
            model.setLag(logEndOffsetAll - offsetAll);
            model.setCreated(CalendarUtils.getNormalDate());
            consumerInfoResult.add(model);
        }
        if (consumer != null) {
            consumer.close();
        }
        result.put(KEY_TOPIC, consumerInfoResult);
        result.put(KEY_PARTITION, offsetModelResult);
        return result;
    }

    public static List<String> getConsumers(String brokers) {
        List<String> list = new ArrayList<String>();
        AdminClient adminClient = createAdminClient(brokers);
        scala.collection.immutable.Map<Node, scala.collection.immutable.List<GroupOverview>> nodeListMap = adminClient.listAllConsumerGroups();
        Iterator<Tuple2<Node, scala.collection.immutable.List<GroupOverview>>> iterator = nodeListMap.iterator();
        while (iterator.hasNext()) {
            Tuple2<Node, scala.collection.immutable.List<GroupOverview>> next = iterator.next();
            scala.collection.immutable.List<GroupOverview> groupOverviewList = next._2();
            List<Object> objects = JavaConversions.bufferAsJavaList(groupOverviewList.toBuffer());
            for (Object obj : objects) {
                list.add(((GroupOverview) obj).groupId());
            }
        }
        return list;
    }

    private static AdminClient createAdminClient(String brokers) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
        AdminClient adminClient = AdminClient.create(props);
        return adminClient;
    }

    public static List<String> getConsumersByCommand(String brokers) {
        Process process = null;
        List<String> processList = new ArrayList<String>();
        try {
            process = Runtime.getRuntime().exec("/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "+brokers+" --list --new-consumer");
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = "";
            while ((line = input.readLine()) != null) {
                if (line != null && !line.isEmpty())
                processList.add(line);
            }
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return processList;
    }
}
