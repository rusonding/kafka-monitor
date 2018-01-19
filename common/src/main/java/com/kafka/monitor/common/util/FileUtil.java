package com.kafka.monitor.common.util;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by lixun on 2017/8/27.
 */
public class FileUtil {
    public static void writeTime(String file, String time) throws IOException {
        FileWriter fw = new FileWriter(file, false);
        fw.write(time);
        fw.close();
    }
}
