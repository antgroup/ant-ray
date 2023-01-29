package io.ray.streaming.common.utils;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/** Util tools for time value operation. */
public class TimeUtil {

  public static final DateTimeFormatter DATE_FORMAT =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

  public static long strTimeToLong(String second) {
    return DateTime.parse(second, DATE_FORMAT).getMillis();
  }

  /** Millis seconds to yyyy-MM-dd HH:mm:ss. */
  public static String longToStrTime(long second) {
    return new DateTime(second).toString(DATE_FORMAT);
  }

  /** Millis seconds to yyyy-MM-dd HH:mm:ss. */
  public static String longToStrTime(long second, DateTimeFormatter formatter) {
    return new DateTime(second).toString(formatter);
  }

  public static String unixTimeToStrTime(long milliSecond) {
    return new DateTime(milliSecond).toString(DATE_FORMAT);
  }

  public static String unixTimeToStrTime() {
    return new DateTime(System.currentTimeMillis()).toString(DATE_FORMAT);
  }

  public static String getReadableTime(long milliSecond) {
    return milliSecond + "[" + TimeUtil.unixTimeToStrTime(milliSecond) + "]";
  }
}
