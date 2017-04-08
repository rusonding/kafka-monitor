package com.kafka.monitor.common.jmx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;

/**
 * Created by lixun on 2017/4/5.
 */
public class KafkaJmxProvider {
    protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
    private static final String MESSAGE_IN_PER_SEC = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec";//所有的topic的消息速率(消息数/秒)
    private static final String BYTES_IN_PER_SEC = "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec";//所有的topic的流入数据速率(字节/秒)
    private static final String BYTES_OUT_PER_SEC = "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec";
    private static final String PRODUCE_REQUEST_PER_SEC = "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce";
    private static final String CONSUMER_REQUEST_PER_SEC = "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=FetchConsumer";
    private static final String FLOWER_REQUEST_PER_SEC = "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=FetchFollower";
    private static final String PRODUCE_TOTAL_TIME_MS = "kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce";
    private static final String CONSUMER_TOTAL_TIME_MS = "kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer";
    private static final String FLOWER_TOTAL_TIME_MS = "kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchFollower";

    private static final String GROUPCOORDINATOR_PER_SEC = "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=GroupCoordinator";
    private static final String OFFSETFETCH_PER_SEC = "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=OffsetFetch";
    private static final String OFFSETS_PER_SEC = "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Offsets";
    private static final String BYTESREJECTED_PER_SEC = "kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec";
    private static final String FAILEDPRODUCEREQUESTS_PER_SEC = "kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec";

    private static final String ACTIVE_CONTROLLER_COUNT = "kafka.controller:type=KafkaController,name=ActiveControllerCount";
    private static final String PART_COUNT = "kafka.server:type=ReplicaManager,name=PartitionCount";
//    private static final String LAG = "kafka.consumer:type=consumer-fetch-manager-metrics,client-id={client-id}";
//    private static final String LAG = "kafka.consumer:type=consumer-fetch-manager-metrics,client-id=([-.w]+)";
    public String extractMonitorData(String host) throws Exception {
        KafkaRoleInfo monitorDataPoint = new KafkaRoleInfo();
        String jmxURL = "service:jmx:rmi:///jndi/rmi://"+host+":8888/jmxrmi";

        try {
            MBeanServerConnection jmxConnection = getMBeanServerConnection(jmxURL);

            ObjectName messageCountObj = new ObjectName(MESSAGE_IN_PER_SEC);
            ObjectName bytesInPerSecObj = new ObjectName(BYTES_IN_PER_SEC);
            ObjectName bytesOutPerSecObj = new ObjectName(BYTES_OUT_PER_SEC);
            ObjectName produceRequestsPerSecObj = new ObjectName(PRODUCE_REQUEST_PER_SEC);
            ObjectName consumerRequestsPerSecObj = new ObjectName(CONSUMER_REQUEST_PER_SEC);
            ObjectName flowerRequestsPerSecObj = new ObjectName(FLOWER_REQUEST_PER_SEC);
            ObjectName produceRequestsTotalTimeObj = new ObjectName(PRODUCE_TOTAL_TIME_MS);
            ObjectName consumerRequestsTotalTimeObj = new ObjectName(CONSUMER_TOTAL_TIME_MS);
            ObjectName flowerRequestsTotalTimeObj = new ObjectName(FLOWER_TOTAL_TIME_MS);

            ObjectName groupCoordinatorPerSecObj = new ObjectName(GROUPCOORDINATOR_PER_SEC);
            ObjectName offsetFetchPerSecObj = new ObjectName(OFFSETFETCH_PER_SEC);
            ObjectName offsetsPerSecObj = new ObjectName(OFFSETS_PER_SEC);
            ObjectName bytesRejectedPerSecObj = new ObjectName(BYTESREJECTED_PER_SEC);
            ObjectName failedProduceRequestsPerSecObj = new ObjectName(FAILEDPRODUCEREQUESTS_PER_SEC);

            ObjectName activeControllerCountObj = new ObjectName(ACTIVE_CONTROLLER_COUNT);
            ObjectName partCountObj = new ObjectName(PART_COUNT);

            Long messagesInPerSec = (Long) jmxConnection.getAttribute(messageCountObj, "Count");
            Long bytesInPerSec = (Long) jmxConnection.getAttribute(bytesInPerSecObj, "Count");
            Long bytesOutPerSec = (Long) jmxConnection.getAttribute(bytesOutPerSecObj, "Count");
            Long produceRequestCountPerSec = (Long) jmxConnection.getAttribute(produceRequestsPerSecObj, "Count");
            Long consumerRequestCountPerSec = (Long) jmxConnection.getAttribute(consumerRequestsPerSecObj, "Count");
            Long flowerRequestCountPerSec = (Long) jmxConnection.getAttribute(flowerRequestsPerSecObj, "Count");
            Long produceRequestCountTotalTime = (Long) jmxConnection.getAttribute(produceRequestsTotalTimeObj, "Count");
            Long consumerRequestCountTotalTime = (Long) jmxConnection.getAttribute(consumerRequestsTotalTimeObj, "Count");
            Long flowerRequestCountTotalTime = (Long) jmxConnection.getAttribute(flowerRequestsTotalTimeObj, "Count");

            Long groupCoordinatorPerSec = (Long) jmxConnection.getAttribute(groupCoordinatorPerSecObj, "Count");
            Long offsetFetchPerSec = (Long) jmxConnection.getAttribute(offsetFetchPerSecObj, "Count");
            Long offsetsPerSec = (Long) jmxConnection.getAttribute(offsetsPerSecObj, "Count");
            Long bytesRejectedPerSec = (Long) jmxConnection.getAttribute(bytesRejectedPerSecObj, "Count");
            Long failedProduceRequestsPerSec = (Long) jmxConnection.getAttribute(failedProduceRequestsPerSecObj, "Count");

            Integer activeControllerCount = (Integer) jmxConnection.getAttribute(activeControllerCountObj, "Value");
            Integer partCount = (Integer) jmxConnection.getAttribute(partCountObj, "Value");

            monitorDataPoint.setMessagesInPerSec(messagesInPerSec);
            monitorDataPoint.setBytesInPerSec(bytesInPerSec);
            monitorDataPoint.setBytesOutPerSec(bytesOutPerSec);
            monitorDataPoint.setProduceRequestCountPerSec(produceRequestCountPerSec);
            monitorDataPoint.setConsumerRequestCountPerSec(consumerRequestCountPerSec);
            monitorDataPoint.setFlowerRequestCountPerSec(flowerRequestCountPerSec);
            monitorDataPoint.setActiveControllerCount(activeControllerCount);
            monitorDataPoint.setPartCount(partCount);
            monitorDataPoint.setProduceRequestCountTotalTime(produceRequestCountTotalTime);
            monitorDataPoint.setConsumerRequestCountTotalTime(consumerRequestCountTotalTime);
            monitorDataPoint.setFlowerRequestCountTotalTime(flowerRequestCountTotalTime);
            monitorDataPoint.setGroupCoordinatorPerSec(groupCoordinatorPerSec);
            monitorDataPoint.setOffsetFetchPerSec(offsetFetchPerSec);
            monitorDataPoint.setOffsetsPerSec(offsetsPerSec);
            monitorDataPoint.setBytesRejectedPerSec(bytesRejectedPerSec);
            monitorDataPoint.setFailedProduceRequestsPerSec(failedProduceRequestsPerSec);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return monitorDataPoint.toString();
    }

    public MBeanServerConnection getMBeanServerConnection(String jmxUrl) throws IOException {
        JMXServiceURL url = new JMXServiceURL(jmxUrl);
        JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
        MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
        return mbsc;
    }

    public static void main(String[] args) throws Exception {
        //        String jmxURL = "service:jmx:rmi:///jndi/rmi://172.17.2.34:8888/jmxrmi";
        //        String jmxURL = "service:jmx:rmi:///jndi/rmi://172.17.2.25:8888/jmxrmi";
//        String hosts = args[0];
        String hosts = "172.17.2.25";
        System.out.println(new KafkaJmxProvider().extractMonitorData(hosts));
    }
}

