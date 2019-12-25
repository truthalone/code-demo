package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * @author ming
 * @date 2019/12/24 10:38
 */
public class DateUtils {

    /**
     * 将UTC时间转换为国内标准时间格式
     * 原始时间格式： EEE MMM dd HH:mm:ss zzz yyyy   Sat Feb 02 20:04:00 CST 2008
     * @param date
     * @return
     */
    public static String ConvertUTCToStandardDate(String date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
        try {
            Date d = dateFormat.parse(date);
            if (d == null) {
                return null;
            }

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return format.format(d);
        } catch (ParseException e) {
            return null;
        }
    }

}
