package com.xskj.manage.datamiddle.common.utils;

import org.springframework.util.StringUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Utils {

    /**
     * 对字符串进行Md5加密
     *
     * @param input 原文
     * @return md5后的密文
     */
    public static String generate(String input) {
        byte[] code = null;
        try {
            code = MessageDigest.getInstance("MD5").digest(input.getBytes());

            return bytesToHex(code);
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }

    /**
     * 对字符串进行Md5加密
     *
     * @param input 原文
     * @param salt  随机数
     * @return string
     */
    public static String generate(String input, String salt) {
        if (StringUtils.isEmpty(salt)) {
            salt = "";
        }

        return generate(salt + generate(input));
    }


    /**
     * 二进制转十六进制
     *
     * @param bytes
     * @return
     */
    public static String bytesToHex(byte[] bytes) {
        StringBuffer md5str = new StringBuffer();
        // 把数组每一字节换成16进制连成md5字符串
        int digital;
        for (int i = 0; i < bytes.length; i++) {
            digital = bytes[i];

            if (digital < 0) {
                digital += 256;
            }
            if (digital < 16) {
                md5str.append("0");
            }
            md5str.append(Integer.toHexString(digital));
        }
        return md5str.toString().toUpperCase();
    }


    public static void main(String[] args) {
        System.out.println(generate(""));
    }

}
