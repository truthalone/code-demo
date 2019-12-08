/**
 * @author ming
 * @date 2019/12/8 15:54
 */

/**
 * 谷歌身份验证器服务端实现
 */

public class Application {
    public static void main(String[] args) {
//        String secret = GoogleAuthenticator.generateSecretKey();
        String secret = "KXZLM3LGEQ2MNKRT";
        System.out.println(secret);
        GoogleAuthenticator googleAuthenticator = new GoogleAuthenticator();

       boolean bret = googleAuthenticator.checkCode(secret,420049,System.currentTimeMillis());
       System.out.println(bret);
    }
}
