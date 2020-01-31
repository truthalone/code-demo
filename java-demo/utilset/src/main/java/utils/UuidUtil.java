package utils;

import java.util.UUID;

/**
 * UUID工具类
 */
public class UuidUtil {
    public static String generate() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    public static void main(String[] args) {
        System.out.println(generate());
    }
}
