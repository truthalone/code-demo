import bean.FileMeta;

/**
 * @author ming
 * @desc
 */
public class Application {
    public static void main(String[] args) {
        FileMeta[] fileMetas = new FileMeta[]{
                new FileMeta("group_data.csv", "GBK", true),
                new FileMeta("posbyadid.csv", "utf-8", false),
                new FileMeta("合作社信息.csv", "utf-8", true),
                new FileMeta("超市数据.csv", "GBK", true)
        };

        HBaseDao.init("host51.ehl.com:60010", "host51.ehl.com,host52.ehl.com,host53.ehl.com", "2181");
        HBaseDao hBaseDao = HBaseDao.getInstance();

        //保存文件
//        Application.saveFile(hBaseDao,fileMetas);

        //查询文件
        hBaseDao.readFile("ming_file_table", "65a77c090a41431dbebe5af5497ff085");

        try {
            hBaseDao.close();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }


    //存储文件
    public static void saveFile(HBaseDao hBaseDao, FileMeta[] fileMetas) {
//        fileName:group_data.csv encoding:GBK catalog:ming_file_table feature:ea90df3de4634e73932f5f42592aa161  rowCount:52 columnCount:2
//        fileName:posbyadid.csv encoding:utf-8 catalog:ming_file_table feature:65a77c090a41431dbebe5af5497ff085  rowCount:58328 columnCount:4
//        fileName:合作社信息.csv encoding:utf-8 catalog:ming_file_table feature:60b2376141394037a6b6487e9004bb7a  rowCount:78 columnCount:14
//        fileName:超市数据.csv encoding:GBK catalog:ming_file_table feature:ae442ed71ff240b69d5688e2610c51c9  rowCount:10000 columnCount:20

        for (FileMeta fileMeta : fileMetas) {
            FileMeta meta = hBaseDao.saveFile(fileMeta);
            System.out.println(meta);
        }
    }

    //查询文件
    public static void readFile(HBaseDao hBaseDao, String catalog, String feature) {
        hBaseDao.readFile(catalog, feature);
    }

}
