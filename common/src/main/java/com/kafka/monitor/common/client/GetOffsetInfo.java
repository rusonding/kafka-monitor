package com.kafka.monitor.common.client;

import com.alibaba.fastjson.JSONObject;
import com.kafka.monitor.common.ConsoleConsumerCommand;


import com.kafka.monitor.common.ConsumerGroupCommand;
import com.kafka.monitor.common.model.ConsumerInfoModel;
import com.kafka.monitor.common.model.KafkaPartitionModel;
import com.kafka.monitor.common.model.OffsetDomain;
import com.kafka.monitor.common.util.*;
import kafka.api.OffsetRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
* Created by lixun on 2017/3/22.
*/
public class GetOffsetInfo {
//    public static final String broker = SystemConfigUtils.getProperty("kafka.broker.list");
    public static final String broker = "node1:9092";


    public static List<ConsumerInfoModel> getNewOffsetInfoVersion2() throws Exception {

        List<String> consumerList = ConsumerGroupCommand.getConsumers(broker);
        List<ConsumerInfoModel> result = new ArrayList<ConsumerInfoModel>();
        for (String consumer : consumerList) {

            Map<String, Object>  map = ConsumerGroupCommand.describeGroup(broker, consumer);
            List<ConsumerInfoModel>  consumerInfoModels = (List<ConsumerInfoModel>)map.get(ConsumerGroupCommand.KEY_TOPIC);
            result.addAll(consumerInfoModels);

            List<OffsetDomain>  partitionModels = (List<OffsetDomain>)map.get(ConsumerGroupCommand.KEY_PARTITION);
            //
            for (OffsetDomain mode : partitionModels) {
                JSONObject obj = new JSONObject();
                obj.put("offsets", mode.getOffset());
                obj.put("modify", CalendarUtils.getNormalDate());
                obj.put("create", CalendarUtils.getNormalDate());
                obj.put("owner", mode.getOwner());
                obj.put("logsize", mode.getLogSize());
                obj.put("lag", mode.getLogSize() - mode.getOffset());
                String group = mode.getOwner();
                String topic = mode.getTopic();
                int paritition = mode.getPartition();
                DBZKDataUtils.updateConsumerOffset(obj.toJSONString(), "/"+group+"/"+topic+"/"+paritition);
            }

        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        List<ConsumerInfoModel> newOffsetInfo = getNewOffsetInfoVersion2();
        System.out.println(newOffsetInfo);
    }
}
