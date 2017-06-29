package com.kafka.monitor.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kafka.monitor.common.model.AlarmDomain;
import com.kafka.monitor.common.model.OffsetsSQLiteDomain;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class DBZKDataUtils
{
  private static final Logger LOG = LoggerFactory.getLogger(DBZKDataUtils.class);
  private static ZKPoolUtils zkPool = ZKPoolUtils.getInstance();
  private static ZkClient zkc = null;
  public static final String ANOTHERKAFKAMONITOR_PATH = "/anotherkafkamonitor";
  private static final String CONSUMER_PARTITION_OFFSET_PATH = "consumer_offset";
  private static final String CONSUMER_PARTITION_STARTING_OFFSET_PATH = "starting_offset";

  private static ZkConnection zkConnection = new ZkConnection(ZKPoolUtils.zkInfo);

  public static String getAlarm()
  {
    Iterator<?> localIterator1;
    String group;
    JSONArray array = new JSONArray();
    if (zkc == null) {
      zkc = zkPool.getZkClient();
    }
    String path = ANOTHERKAFKAMONITOR_PATH+"/alarm";
    ZkUtils zkUtils = new ZkUtils(zkc, zkConnection, false);

    if (zkUtils.pathExists(path)) {
      Seq<?> seq = zkUtils.getChildren(path);
      List<?> listSeq = JavaConversions.seqAsJavaList(seq);
      for (localIterator1 = listSeq.iterator(); localIterator1.hasNext(); ) { group = (String)localIterator1.next();
        Seq<String> seq2 = zkUtils.getChildren(path + "/" + group);
        List<String> listSeq2 =JavaConversions.seqAsJavaList(seq2);
        for (String topic : listSeq2) {
          try {
            JSONObject object = new JSONObject();
            object.put("group", group);
            object.put("topic", topic);
            Tuple2<?, ?> tuple = zkUtils.readDataMaybeNull(path + "/" + group + "/" + topic);
            object.put("created", CalendarUtils.timeSpan2StrDate(((Stat)tuple._2()).getCtime()));
            object.put("modify", CalendarUtils.timeSpan2StrDate(((Stat)tuple._2()).getMtime()));
            long lag = JSON.parseObject((String)((Option<?>)tuple._1()).get()).getLong("lag").longValue();
            String owner = JSON.parseObject((String)((Option<?>)tuple._1()).get()).getString("owner");
            object.put("lag", Long.valueOf(lag));
            object.put("owner", owner);
            array.add(object);
          } catch (Exception ex) {
            LOG.error("[ZK.getAlarm] has error,msg is ",ex);
          }
        }
      }
    }
    if (zkc != null) {
      zkPool.release(zkc);
      zkc = null;
    }
    return array.toJSONString();
  }

  public static String getOffsets(String group, String topic) {
    String data = "";
    if (zkc == null) {
      zkc = zkPool.getZkClient();
    }
    ZkUtils zkUtils = new ZkUtils(zkc, zkConnection, false);
    String path = ANOTHERKAFKAMONITOR_PATH+"/offsets/" + group + "/" + topic;
    if (zkUtils.pathExists(path)) {
      try {
        Tuple2<?, ?> tuple = zkUtils.readDataMaybeNull(path);
        JSONObject obj = JSON.parseObject((String)((Option<?>)tuple._1()).get());
        if (CalendarUtils.getZkHour().equals(obj.getString("hour")))
          data = obj.toJSONString();
      }
      catch (Exception ex) {
        LOG.error("[ZK.getOffsets] has error,msg is " + ex.getMessage());
      }
    }
    if (zkc != null) {
      zkPool.release(zkc);
      zkc = null;
    }
    return data;
  }

  public static JSONObject getConsumerOffsets(String group, String topic, String partition) {
    JSONObject obj = new JSONObject();
    if (zkc == null) {
      zkc = zkPool.getZkClient();
    }
    ZkUtils zkUtils = new ZkUtils(zkc, zkConnection, false);
    String path = ANOTHERKAFKAMONITOR_PATH+"/"+CONSUMER_PARTITION_OFFSET_PATH+"/" + group + "/" + topic + "/" + partition;
    if (zkUtils.pathExists(path)) {
      try {
        Tuple2<?, ?> tuple = zkUtils.readDataMaybeNull(path);
        obj = JSON.parseObject((String)((Option<?>)tuple._1()).get());
      }
      catch (Exception ex) {
        LOG.error("[ZK.getOffsets] has error,msg is " + ex.getMessage());
      }
    }
    if (zkc != null) {
      zkPool.release(zkc);
      zkc = null;
    }
    return obj;
  }

  public static long getConsumerStartingOffsets(String group) {
    long offset = 0;
    if (zkc == null) {
      zkc = zkPool.getZkClient();
    }
    ZkUtils zkUtils = new ZkUtils(zkc, zkConnection, false);
    String path = ANOTHERKAFKAMONITOR_PATH+"/"+CONSUMER_PARTITION_STARTING_OFFSET_PATH+"/" + group;
    if (zkUtils.pathExists(path)) {
      try {
        Tuple2<Option<String>, Stat> tuple = zkUtils.readDataMaybeNull(path);
        offset = Long.parseLong(tuple._1().get());
      }
      catch (Exception ex) {
        LOG.error("[ZK.getOffsets] has error,msg is " + ex.getMessage());
      }
    }
    if (zkc != null) {
      zkPool.release(zkc);
      zkc = null;
    }
    return offset;
  }

  private static void update(String data, String path) {
    if (zkc == null) {
      zkc = zkPool.getZkClient();
    }
    ZkUtils zkUtils = new ZkUtils(zkc, zkConnection, false);
    List<ACL> acls = zkUtils.DefaultAcls();
    if (!(zkUtils.pathExists(ANOTHERKAFKAMONITOR_PATH+"/" + path))) {
      zkUtils.createPersistentPath(ANOTHERKAFKAMONITOR_PATH+"/" + path, "", acls);
    }
    if (zkUtils.pathExists(ANOTHERKAFKAMONITOR_PATH+"/" + path)) {
      zkUtils.updatePersistentPath(ANOTHERKAFKAMONITOR_PATH+"/" + path, data, acls);
    }
    if (zkc != null) {
      zkPool.release(zkc);
      zkc = null;
    }
  }

  public static void updateConsumerOffset(String data, String path) {
    update(data, CONSUMER_PARTITION_OFFSET_PATH + path);
  }

  public static void updateConsumerStartingOffset(String data, String path) {
    update(data, CONSUMER_PARTITION_STARTING_OFFSET_PATH + path);
  }


  public static void insert(List<OffsetsSQLiteDomain> list) {
    String hour = CalendarUtils.getZkHour();
    for (OffsetsSQLiteDomain offset : list) {
      JSONObject obj = new JSONObject();
      obj.put("hour", hour);

      JSONObject object = new JSONObject();
      object.put("lag", Long.valueOf(offset.getLag()));
      object.put("lagsize", Long.valueOf(offset.getLogSize()));
      object.put("offsets", Long.valueOf(offset.getOffsets()));
      object.put("created", offset.getCreated());
      String json = getOffsets(offset.getGroup(), offset.getTopic());
      JSONObject tmp = JSON.parseObject(json);
      JSONArray zkArrayData = new JSONArray();
      if ((tmp != null) && (tmp.size() > 0)) {
        String zkHour = tmp.getString("hour");
        if (hour.equals(zkHour)) {
          String zkData = tmp.getString("data");
          zkArrayData = JSON.parseArray(zkData);
        }
      }
      if (zkArrayData.size() > 0) {
        zkArrayData.add(object);
        obj.put("data", zkArrayData);
      } else {
        obj.put("data", Arrays.asList(new JSONObject[]{object}));
      }
      update(obj.toJSONString(), "offsets/" + offset.getGroup() + "/" + offset.getTopic());
    }
  }

  public static void delete(String group, String topic, String theme) {
    if (zkc == null) {
      zkc = zkPool.getZkClient();
    }
    ZkUtils zkUtils = new ZkUtils(zkc, zkConnection, false);

    String path = theme + "/" + group + "/" + topic;
    if (zkUtils.pathExists(ANOTHERKAFKAMONITOR_PATH+"/" + path)) {
      zkUtils.deletePath(ANOTHERKAFKAMONITOR_PATH+"/" + path);
    }
    if (zkc != null) {
      zkPool.release(zkc);
      zkc = null;
    }
  }

  public static int insertAlarmConfigure(AlarmDomain alarm) {
    JSONObject object = new JSONObject();
    object.put("lag", Long.valueOf(alarm.getLag()));
    object.put("owner", alarm.getOwners());
    try {
      update(object.toJSONString(), "alarm/" + alarm.getGroup() + "/" + alarm.getTopics());
    } catch (Exception ex) {
      LOG.error("[ZK.insertAlarm] has error,msg is " + ex.getMessage());
      return -1;
    }
    return 0;
  }

  public static void main(String[] args) {
    String str = getOffsets("testConsumer1", "logs_app");
    System.out.println(str);
  }
}