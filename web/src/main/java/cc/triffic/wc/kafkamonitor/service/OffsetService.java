package cc.triffic.wc.kafkamonitor.service;

import cc.triffic.wc.kafkamonitor.domain.TupleDomain;
import cc.triffic.wc.kafkamonitor.utils.LRUCacheUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kafka.monitor.common.GetOffsetShellCommand;
import com.kafka.monitor.common.model.OffsetDomain;
import com.kafka.monitor.common.model.OffsetZkDomain;
import com.kafka.monitor.common.model.TopicPartitionOffsetModel;
import com.kafka.monitor.common.util.DBZKDataUtils;
import com.kafka.monitor.common.util.KafkaClusterUtils;

import java.util.*;

public class OffsetService
{
  private static LRUCacheUtils<String, TupleDomain> lruCache = new LRUCacheUtils<String, TupleDomain>(100000);

  public static String getLogSize(String topic, String group, String ip) {

    List<OffsetDomain> list = new ArrayList<OffsetDomain>();
    //如果new api已经有了，不添加old api
    List<String> temp = new ArrayList<String>();
    //new api
    List<TopicPartitionOffsetModel> topicPartitionOffset = GetOffsetShellCommand.getTopicPartitionOffset(topic);
    for (TopicPartitionOffsetModel model : topicPartitionOffset) {
      int partition = model.getPartition();
      long logSize = model.getOffset();
      JSONObject consumerOffsets = DBZKDataUtils.getConsumerOffsets(group, topic, partition + "");
      if (!consumerOffsets.isEmpty()) {
        long offsets = Long.parseLong(consumerOffsets.getString("offsets"));
        String create = consumerOffsets.get("create")+"";
        String modify = consumerOffsets.get("modify")+"";
        OffsetDomain offset = new OffsetDomain();
        offset.setPartition(partition);
        offset.setLogSize(logSize);
        offset.setOffset(offsets);
        offset.setCreate(create);
        offset.setModify(modify);
        offset.setLag(logSize - offsets);
        offset.setOwner("");
        list.add(offset);
        temp.add(group+topic);
      }
    }

    //old api
    List<String> hosts = getBrokers(topic, group, ip);
    List<String> partitions = KafkaClusterUtils.findTopicPartition(topic);
    if (!temp.contains(group+topic)) {
      for (String partition : partitions) {
        int partitionInt = Integer.parseInt(partition);
        OffsetZkDomain offsetZk = KafkaClusterUtils.getOffset(topic, group, partitionInt);
        OffsetDomain offset = new OffsetDomain();
        long logSize = KafkaClusterUtils.getLogSize(hosts, topic, partitionInt);
        offset.setPartition(partitionInt);
        offset.setLogSize(logSize);
        offset.setCreate(offsetZk.getCreate());
        offset.setModify(offsetZk.getModify());
        offset.setOffset(offsetZk.getOffset());
        offset.setLag((offsetZk.getOffset() == -1L) ? 0L : logSize - offsetZk.getOffset());
        offset.setOwner(offsetZk.getOwners());
        list.add(offset);
      }
    }
    return list.toString();
  }

  @Deprecated
  public static String getOldLogSize(String topic, String group, String ip) {
    List<String> hosts = getBrokers(topic, group, ip);
    List<String> partitions = KafkaClusterUtils.findTopicPartition(topic);
    List<OffsetDomain> list = new ArrayList<OffsetDomain>();
    for (String partition : partitions) {
      int partitionInt = Integer.parseInt(partition);
      OffsetZkDomain offsetZk = KafkaClusterUtils.getOffset(topic, group, partitionInt);
      OffsetDomain offset = new OffsetDomain();
      long logSize = KafkaClusterUtils.getLogSize(hosts, topic, partitionInt);
      offset.setPartition(partitionInt);
      offset.setLogSize(logSize);
      offset.setCreate(offsetZk.getCreate());
      offset.setModify(offsetZk.getModify());
      offset.setOffset(offsetZk.getOffset());
      offset.setLag((offsetZk.getOffset() == -1L) ? 0L : logSize - offsetZk.getOffset());
      offset.setOwner(offsetZk.getOwners());
      list.add(offset);
    }
    return list.toString();
  }

  private static List<String> getBrokers(String topic, String group, String ip)
  {
    TupleDomain tuple;
    String key = group + "_" + topic + "_consumer_brokers_" + ip;
    String brokers = "";
    if (lruCache.containsKey(key)) {
      tuple = (TupleDomain)lruCache.get(key);
      brokers = tuple.getRet();
      long end = System.currentTimeMillis();
      if ((end - tuple.getTimespan()) / 60000.0D > 3.0D)
        lruCache.remove(key);
    }
    else {
      brokers = KafkaClusterUtils.getAllBrokersInfo();
      tuple = new TupleDomain();
      tuple.setRet(brokers);
      tuple.setTimespan(System.currentTimeMillis());
      lruCache.put(key, tuple);
    }
    JSONArray arr = JSON.parseArray(brokers);
    List<String> list = new ArrayList<String>();
    for (Iterator<?> localIterator = arr.iterator(); localIterator.hasNext(); ) { Object object = localIterator.next();
      JSONObject obj = (JSONObject)object;
      String host = obj.getString("host");
      int port = obj.getInteger("port").intValue();
      list.add(host + ":" + port);
    }
    return list;
  }

  public static boolean isGroupTopic(String group, String topic, String ip) {
    TupleDomain tuple;
    String key = group + "_" + topic + "_consumer_owners_" + ip;
    boolean status = false;
    if (lruCache.containsKey(key)) {
      tuple = (TupleDomain)lruCache.get(key);
      status = tuple.isStatus();
      long end = System.currentTimeMillis();
      if ((end - tuple.getTimespan()) / 60000.0D > 3.0D)
        lruCache.remove(key);
    }
    else {
      status = KafkaClusterUtils.findTopicIsConsumer(topic, group);
      tuple = new TupleDomain();
      tuple.setStatus(status);
      tuple.setTimespan(System.currentTimeMillis());
      lruCache.put(key, tuple);
    }
    return status;
  }

  public static String getOffsetsGraph(String group, String topic) {
    String ret = DBZKDataUtils.getOffsets(group, topic);
    if (ret.length() > 0) {
      ret = JSON.parseObject(ret).getString("data");
    }
    return ret;
  }

  public static void main(String[] args) {
    String logSize = OffsetService.getLogSize("bi_useractionlogs_app", "bi_useractionlogs_app_flume", "localhost");
    System.out.println(logSize);
  }
}