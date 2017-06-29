package com.kafka.monitor.common.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lixun on 2017/3/21.
 */
public class GroupHashCode {
    public static void main(String[] args) {
        System.out.println(getPartition("app_flume"));
//        System.out.println(getPartition("testConsumer18"));
//        List<Integer> list = new ArrayList<Integer>();
//        for (int i = 5; i < 57; i++) {
//            String c = "testConsumer"+i;
//            int partition = getPartition(c);
//
//            if (list.contains(partition)) {
//                System.out.println("list="+list);
//                System.out.println(c);
//            } else {
//                list.add(partition);
//            }
//        }
    }

    public static int getPartition(String group) {
        return Math.abs(group.hashCode()) % 50;
    }
}
