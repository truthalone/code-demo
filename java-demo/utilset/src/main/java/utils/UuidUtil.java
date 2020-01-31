package com.xskj.manage.dataexchange.common.utils;

import java.util.UUID;

/**
 * UUID工具类
 */
public class UuidUtil {
    public static String generate() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
