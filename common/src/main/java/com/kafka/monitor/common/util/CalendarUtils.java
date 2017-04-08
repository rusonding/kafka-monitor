package com.kafka.monitor.common.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by lixun on 2017/3/21.
 */
public class CalendarUtils {
    public static String getCurrentEndDate() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        return df.format(new Date());
    }

    public static String getCurrentStartDate() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:00");
        return df.format(new Date());
    }

    public static String getStatsPerDate() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        return df.format(new Date());
    }

    public static String getYestoday() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd 23:59:59");
        Calendar calendar = Calendar.getInstance();
        Date curr = new Date();
        calendar.setTime(curr);
        calendar.add(5, -1);
        return df.format(calendar.getTime());
    }

    public static long getTime() {
        return new Date().getTime();
    }

    public static String getLastDay() {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        Date curr = new Date();
        calendar.setTime(curr);
        calendar.add(5, -1);
        return df.format(calendar.getTime());
    }

    public static String[] getLastMonth() {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        Calendar calendarFirstDay = Calendar.getInstance();
        calendarFirstDay.add(2, -1);
        calendarFirstDay.set(5, 1);
        Calendar calendarLastDay = Calendar.getInstance();
        calendarLastDay.set(5, 1);
        calendarLastDay.add(5, -1);
        return new String[] { df.format(calendarFirstDay.getTime()),
                df.format(calendarLastDay.getTime()) };
    }

    public static String timeSpan2StrDate(long date) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(new Date(date));
    }

    public static String getNormalDate() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(new Date());
    }

    public static String getZkHour() {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");
        return df.format(new Date());
    }

}
