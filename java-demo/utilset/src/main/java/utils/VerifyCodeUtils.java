package utils;

import java.util.Random;

/**
 * 验证码工具类
 */
public class VerifyCodeUtils {
    private static final String VERIFY_CODES = "0123456789";
    private static final int VERIFY_CODE_LENGTH = 6;

    /**
     * 生成验证码
     *
     * @return
     */
    public static String generateVerifyCode() {
        return generateVerifyCode(VERIFY_CODE_LENGTH, VERIFY_CODES);
    }

    /**
     * 使用指定源生成验证码
     *
     * @param verifySize 验证码长度
     * @param sources    验证码字符源
     * @return
     */
    public static String generateVerifyCode(int verifySize, String sources) {
        if (sources == null || sources.length() == 0) {
            sources = VERIFY_CODES;
        }
        int codesLen = sources.length();
        Random rand = new Random(System.currentTimeMillis());
        StringBuilder verifyCode = new StringBuilder(verifySize);
        for (int i = 0; i < verifySize; i++) {
            verifyCode.append(sources.charAt(rand.nextInt(codesLen - 1)));
        }

        return verifyCode.toString();
    }

    public static void main(String[] args) {
        System.out.println(generateVerifyCode());
    }

}
