/**
 * @author ming
 * @date 2019/12/17 11:52
 */
public class Application {
    public static void main(String[] args) {
        HbaseDao hbaseDao = new HbaseDao();
        String tableName = "mytable";
//        hbaseDao.createTable("mytable");
//        hbaseDao.insertData(tableName);

//        hbaseDao.queryByRowKey("100", tableName);

//        hbaseDao.delete(tableName,"100");


        hbaseDao.queryMultiCameraPath();
    }

}
