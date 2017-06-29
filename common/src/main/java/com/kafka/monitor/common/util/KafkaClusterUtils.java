package com.kafka.monitor.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kafka.monitor.common.model.BrokersDomain;
import com.kafka.monitor.common.model.OffsetZkDomain;
import com.kafka.monitor.common.model.PartitionsDomain;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.*;

/**
 * Created by lixun on 2017/3/21.
 */
public class KafkaClusterUtils {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaClusterUtils.class);
    private static ZKPoolUtils zkPool = ZKPoolUtils.getInstance();
    private static String KAFKA_CONSUMERS_PATH = "/consumers";
    private static String KAFKA_BROKERS_PATH = "/brokers";
    private static String ANOTHER_PATH = "/anotherkafkamonitor";

    private static ZkConnection zkConnection = new ZkConnection(ZKPoolUtils.zkInfo);

    public static void main(String[] args) throws Exception {
        String topic = "logs_app";
    }

    public static OffsetZkDomain getOffset(String topic, String group, int partition) {
        ZkClient zkc = zkPool.getZkClientSerializer();
        ZkUtils zkUtils = new ZkUtils(zkc, zkConnection, false);

        OffsetZkDomain offsetZk = new OffsetZkDomain();
        String offsetPath = KAFKA_CONSUMERS_PATH+"/" + group + "/offsets/" + topic
                + "/" + partition;
        String ownersPath = KAFKA_CONSUMERS_PATH+"/" + group + "/owners/" + topic
                + "/" + partition;

        Tuple2<?, ?> tuple = null;
        try {
            if (zkUtils.pathExists(offsetPath)) {
                tuple = zkUtils.readDataMaybeNull(offsetPath);
            } else {
                LOG.warn("partition[" + partition + "],offsetPath[" + offsetPath + "] is not exist!");

                if (zkc != null) {
                    zkPool.releaseZKSerializer(zkc);
                    zkc = null;
                }
                return offsetZk;
            }
        } catch (Exception ex) {
            LOG.error("partition[" + partition
                    + "],get offset has error,msg is " + ex.getMessage());
            if (zkc != null) {
                zkPool.releaseZKSerializer(zkc);
                zkc = null;
            }
            return offsetZk;
        }
        long offsetSize = Long.parseLong((String) ((Option<?>) tuple._1()).get());

        if (zkUtils.pathExists(ownersPath)) {
            Tuple2<?, ?> tuple2 = zkUtils.readData(ownersPath);
            offsetZk.setOwners((tuple2._1() == null) ? "" : (String) tuple2._1());
        } else {
            offsetZk.setOwners("");
        }
        offsetZk.setOffset(offsetSize);
        offsetZk.setCreate(CalendarUtils.timeSpan2StrDate(((Stat) tuple._2())
                .getCtime()));
        offsetZk.setModify(CalendarUtils.timeSpan2StrDate(((Stat) tuple._2())
                .getMtime()));
        if (zkc != null) {
            zkPool.releaseZKSerializer(zkc);
            zkc = null;
        }
        return offsetZk;
    }

    public static boolean findTopicIsConsumer(String topic, String group) {
        ZkClient zkc = zkPool.getZkClient();
        String ownersPath = KAFKA_CONSUMERS_PATH+"/" + group + "/owners/" + topic;
        ZkUtils zkUtils = new ZkUtils(zkc, zkConnection, false);
        boolean status = zkUtils.pathExists(ownersPath);
        if (zkc != null) {
            zkPool.release(zkc);
            zkc = null;
        }
        return status;
    }

    public static String getZkInfo() {
        String[] zks = SystemConfigUtils.getPropertyArray("kafka.zk.list", ",");
        JSONArray arr = new JSONArray();
        int id = 1;
        for (String zk : zks) {
            JSONObject obj = new JSONObject();
            obj.put("id", Integer.valueOf(id++));
            obj.put("ip", zk.split(":")[0]);
            obj.put("port", zk.split(":")[1]);
            obj.put("mode", ZookeeperUtils.serverStatus(zk.split(":")[0],
                    zk.split(":")[1]));
            arr.add(obj);
        }
        return arr.toJSONString();
    }

    public static JSONObject zkCliIsLive() {
        JSONObject object = new JSONObject();
        ZkClient zkc = zkPool.getZkClient();
        if (zkc != null) {
            object.put("live", Boolean.valueOf(true));
            object.put("list", SystemConfigUtils.getProperty("kafka.zk.list"));
        } else {
            object.put("live", Boolean.valueOf(false));
            object.put("list", SystemConfigUtils.getProperty("kafka.zk.list"));
        }
        if (zkc != null) {
            zkPool.release(zkc);
            zkc = null;
        }
        return object;
    }

    public static long getLogSize(List<String> hosts, String topic,
                                  int partition) {
        LOG.debug("Find leader hosts [" + hosts + "]");
        PartitionMetadata metadata = findLeader(hosts, topic, partition);
        if (metadata == null) {
            LOG.error("[KafkaClusterUtils.getLogSize()] - Can't find metadata for Topic and Partition. Exiting");

            return 0L;
        }
        if (metadata.leader() == null) {
            LOG.error("[KafkaClusterUtils.getLogSize()] - Can't find Leader for Topic and Partition. Exiting");

            return 0L;
        }

        String clientName = "Client_" + topic + "_" + partition;
        String reaHost = metadata.leader().host();
        int port = metadata.leader().port();

        long ret = 0L;
        try {
            SimpleConsumer simpleConsumer = new SimpleConsumer(reaHost, port,
                    100000, 65536, clientName);

            TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
                    partition);

            Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
            requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
                    kafka.api.OffsetRequest.LatestTime(), 1));

            kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                    requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
                    clientName);
            OffsetResponse response = simpleConsumer.getOffsetsBefore(request);
            if (response.hasError()) {
                LOG.error("Error fetching data Offset , Reason: "
                        + response.errorCode(topic, partition));
                return 0L;
            }
            long[] offsets = response.offsets(topic, partition);
            ret = offsets[0];
            if (simpleConsumer != null)
                simpleConsumer.close();
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        }
        return ret;
    }


    public static long getLogSizes(List<String> hosts, String topic, int partition) {
        PartitionMetadata metadata = findLeader(hosts, topic, partition);
        if (metadata == null) {
            LOG.error("[KafkaClusterUtils.getLogSize()] - Can't find metadata for Topic and Partition. Exiting");
            return 0L;
        }
        if (metadata.leader() == null) {
            LOG.error("[KafkaClusterUtils.getLogSize()] - Can't find Leader for Topic and Partition. Exiting");
            return 0L;
        }
        String clientName = "client_" + topic + "_" + partition;
        String reaHost = metadata.leader().host();
        int port = metadata.leader().port();
        long ret = 0L;
        try {
            SimpleConsumer simpleConsumer = new SimpleConsumer(reaHost, port, 100000, 65536, clientName);
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
            Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
            requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));
            OffsetRequest request = new OffsetRequest(
                    requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
            OffsetResponse response = simpleConsumer.getOffsetsBefore(request);
            if (response.hasError()) {
                LOG.error("Error fetching data Offset , Reason: " + response.errorCode(topic, partition));
                return 0L;
            }
            long[] offsets = response.offsets(topic, partition);
            ret = offsets[0];
            if (simpleConsumer != null)
                simpleConsumer.close();
        } catch (Exception ex) {
            LOG.error("error:", ex);
        }
        return ret;
    }

    private static PartitionMetadata findLeader(List<String> seedBrokers, String topic, int partition) {
        PartitionMetadata returnMetaData = null;
        for (String seed : seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                String ip = seed.split(":")[0];
                String port = seed.split(":")[1];
                consumer = new SimpleConsumer(ip, Integer.parseInt(port), 10000, 65536, "leaderLookup");
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                TopicMetadataResponse resp = consumer.send(req);
                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData)
                    for (PartitionMetadata part : item.partitionsMetadata())
                        if (part.partitionId() == partition) {
                            returnMetaData = part;
                            if (consumer == null) {
                                return returnMetaData;
                            }
                            consumer.close();
                            return returnMetaData;
                        }
            } catch (Exception e) {
                LOG.error("Error communicating with Broker [" + seed + "] to find Leader for [" + topic + ", " + partition + "] Reason: " + e);
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }
        return returnMetaData;
    }

    public static String getAllPartitions() {
        int id;
        ZkClient zkc = zkPool.getZkClientSerializer();
        ZkUtils zkUtils = new ZkUtils(zkc, zkConnection, false);
        List<PartitionsDomain> list = new ArrayList<PartitionsDomain>();
        if (zkUtils.pathExists(KAFKA_BROKERS_PATH+"/topics")) {
            Seq<String> seq = zkUtils.getChildren(KAFKA_BROKERS_PATH+"/topics");
            List<String> listSeq = JavaConversions.seqAsJavaList(seq);
            id = 0;
            for (String topic : listSeq) {
                try {
                    Tuple2<?, ?> tuple = zkUtils.readDataMaybeNull(KAFKA_BROKERS_PATH+"/topics/" + topic);
                    PartitionsDomain partition = new PartitionsDomain();
                    partition.setId(++id);
                    partition.setCreated(CalendarUtils
                            .timeSpan2StrDate(((Stat) tuple._2()).getCtime()));
                    partition.setModify(CalendarUtils
                            .timeSpan2StrDate(((Stat) tuple._2()).getMtime()));
                    partition.setTopic(topic);

                    JSONObject partitionObject = JSON.parseObject(
                            (String) ((Option<?>) tuple._1()).get())
                            .getJSONObject("partitions");

                    partition.setPartitionNumbers(partitionObject.size());
                    partition.setPartitions(partitionObject.keySet());
                    list.add(partition);
                } catch (Exception ex) {
                    LOG.error(ex.getMessage());
                }
            }
        }
        if (zkc != null) {
            zkPool.releaseZKSerializer(zkc);
            zkc = null;
        }
        return list.toString();
    }

    public static String geyReplicasIsr(String topic, int partitionid) {
        ZkClient zkc = zkPool.getZkClientSerializer();
        ZkUtils zkUtils = new ZkUtils(zkc, zkConnection, false);
        Seq<?> seq = zkUtils.getInSyncReplicasForPartition(topic, partitionid);

        List<?> listSeq = JavaConversions.seqAsJavaList(seq);
        if (zkc != null) {
            zkPool.releaseZKSerializer(zkc);
            zkc = null;
        }
        return listSeq.toString();
    }

    public static String getAllBrokersInfo() {
        int id;
        ZkClient zkc = zkPool.getZkClientSerializer();
        ZkUtils zkUtils = new ZkUtils(zkc, zkConnection, false);

        List<BrokersDomain> list = new ArrayList<BrokersDomain>();
        if (zkUtils.pathExists(KAFKA_BROKERS_PATH+"/ids")) {
            Seq<String> seq = zkUtils.getChildren(KAFKA_BROKERS_PATH+"/ids");
            List<String> listSeq = JavaConversions.seqAsJavaList(seq);
            id = 0;
            for (String ids : listSeq) {
                try {
                    Tuple2<?, ?> tuple = zkUtils.readDataMaybeNull(KAFKA_BROKERS_PATH+"/ids/" + ids);
                    BrokersDomain broker = new BrokersDomain();
                    broker.setCreated(CalendarUtils
                            .timeSpan2StrDate(((Stat) tuple._2()).getCtime()));
                    broker.setModify(CalendarUtils
                            .timeSpan2StrDate(((Stat) tuple._2()).getMtime()));

                    String host = JSON.parseObject(
                            (String) ((Option<?>) tuple._1()).get()).getString(
                            "host");

                    int port = JSON
                            .parseObject((String) ((Option<?>) tuple._1()).get())
                            .getInteger("port").intValue();

                    broker.setHost(host);
                    broker.setPort(port);
                    broker.setId(++id);
                    list.add(broker);
                } catch (Exception ex) {
                    LOG.error("get borkder error=", ex);
                }
            }
        }
        if (zkc != null) {
            zkPool.releaseZKSerializer(zkc);
            zkc = null;
        }
        return list.toString();
    }

    public static List<String> findTopicPartition(String topic) {
        ZkClient zkc = zkPool.getZkClient();
        ZkUtils zkUtils = new ZkUtils(zkc, zkConnection, false);
        Seq<String> seq = zkUtils.getChildren(KAFKA_BROKERS_PATH+"/topics/" + topic + "/partitions");
        List<String> listSeq = JavaConversions.seqAsJavaList(seq);
        if (zkc != null) {
            zkPool.release(zkc);
        }
        return listSeq;
    }

    //kafka0.10.0新版本的，调度实时更新数据到该ANOTHER_PATH目录下
    public static Map<String, List<String>> getConsumers() {
        ZkClient zkc = zkPool.getZkClient();
        ZkUtils zkUtils = new ZkUtils(zkc, zkConnection, false);
        Map<String, List<String>> mapConsumers = new HashMap<String, List<String>>();
        try {
            Seq<String> seq = zkUtils.getChildren(ANOTHER_PATH+"/"+"offsets");
            List<String> listSeq = JavaConversions.seqAsJavaList(seq);
            for (String group : listSeq) {
                try {
                    Seq<String> tmp = zkUtils.getChildren(ANOTHER_PATH+"/offsets/" + group);
                    List<String> list = JavaConversions.seqAsJavaList(tmp);
                    mapConsumers.put(group, list);
                } catch (Exception e ) {
                    LOG.warn("get consumer owners error:"+ e.getMessage());
                }
            }
        } catch (Exception ex) {
            LOG.error("get consumer error:", ex);
        } finally {
            if (zkc != null) {
                zkPool.release(zkc);
                zkc = null;
            }
        }
        return mapConsumers;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Map<String, List<String>> getActiveTopic() {
        Iterator localIterator1;
        ZkClient zkc = zkPool.getZkClientSerializer();
        ZkUtils zkUtils = new ZkUtils(zkc, zkConnection, false);
        Map actvTopic = new HashMap();
        try {
            String group;
            Seq seq = zkUtils.getChildren(KAFKA_CONSUMERS_PATH);
            List listSeq = JavaConversions.seqAsJavaList(seq);
            JSONArray arr = new JSONArray();
            for (localIterator1 = listSeq.iterator(); localIterator1.hasNext();) {
                group = (String) localIterator1.next();

                scala.collection.mutable.Map map = zkUtils.getConsumersPerTopic(group, false);

                for (Map.Entry entry : (Set<Map.Entry<String, Object>>) JavaConversions
                        .mapAsJavaMap(map).entrySet()) {
                    JSONObject obj = new JSONObject();
                    obj.put("topic", entry.getKey());
                    obj.put("group", group);
                    arr.add(obj);
                }
            }
            for (localIterator1 = arr.iterator(); localIterator1.hasNext();) {
                Object object = localIterator1.next();
                JSONObject obj = (JSONObject) object;
                group = obj.getString("group");
                String topic = obj.getString("topic");
                if (actvTopic.containsKey(group + "_" + topic)) {
                    ((List) actvTopic.get(group + "_" + topic)).add(topic);
                } else {
                    List topics = new ArrayList();
                    topics.add(topic);
                    actvTopic.put(group + "_" + topic, topics);
                }
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        } finally {
            if (zkc != null) {
                zkPool.releaseZKSerializer(zkc);
                zkc = null;
            }
        }
        return actvTopic;
    }

}
