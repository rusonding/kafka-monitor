package com.kafka.monitor.consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.kafka.monitor.common.util.FileUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by lixun on 2017/8/27.
 */
public class ConsumerMonitor {
    public static void main(String[] args) {
        connectionKafka();
    }

    @SuppressWarnings("resource")
    public static void connectionKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "ip:9092");
//        props.put("group.id", "testConsumer14");
        long group = System.currentTimeMillis();
        props.put("group.id", "__test"+group);
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("bi_useractionlogs"));
        String file = "/tmp/kafka_useraction_log";
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            boolean flag = false;
            for (ConsumerRecord<String, String> record : records) {
//                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                try {
                    JSONObject json = (JSONObject)JSON.parse(record.value());
                    String createTime = "monitor_createTime=" + (json.get("CreateTime") + "").substring(0, 19);
                    System.out.println(createTime);
                    FileUtil.writeTime(file, createTime);
                    flag = true;
                    break;
                } catch (Exception e) {
                    String time = "monitor_createTime=0";
                    System.out.println(time);
                    try {
                        FileUtil.writeTime(file, time);
                    } catch (IOException e1) {
                    }
                }
            }
//            consumer.commitSync();
            if (flag) {
                consumer.close();
                break;
            }
        }
    }
}
