package utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Scanner;

/**
 * 时间格式化
 */
public class DateUtil<psvm> {
    public static int getHour(long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    public static int getMinute(long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        return calendar.get(Calendar.MINUTE);
    }

    public static String longToStr(String dateFormat, long timestamp) {
        SimpleDateFormat format = new SimpleDateFormat(dateFormat);
        String timeText = format.format(timestamp);
        return timeText;
    }

    public static void main(String[] args) {
        System.out.println(2 << 3);
    }
}
