package com.xskj.manage.datamiddle.common.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 日期工具类
 */
public class DateUtils {

    /**
     * 默认角色资源有效期
     *
     * @return
     */
    public static Date defaultResourceValidDate() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2050, 11, 31, 0, 0, 0);

        return calendar.getTime();
    }


    /**
     * 默认角色过期时间
     *
     * @return
     */
    public static Date defaultUserRoleValidDate() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2050, 11, 31, 0, 0, 0);

        return calendar.getTime();
    }


    /**
     * 获取当前时间
     *
     * @return
     */
    public static String currentDatetime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(new Date());
    }


    /**
     * 将字符串解析成时间对象
     * @param date
     * @return
     */
    public static Date parseDatetime(String date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        if (date.contains(":")) {
            dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }

        try {
            return dateFormat.parse(date);
        } catch (Exception ex) {
            return null;
        }

    }


}
