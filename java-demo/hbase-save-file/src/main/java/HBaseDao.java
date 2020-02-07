import bean.FileMeta;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;
import utils.UuidUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author ming
 * @desc
 */
public class HBaseDao {
    private Configuration configuration;
    private Connection connection = null;
    private static HBaseDao instance;

    private static String maste;
    private static String zks;
    private static String port;

    /**
     * 初始化单例参数
     *
     * @param hbaseMaster hbase master节点信息
     * @param zookeepers  zookeepers 连接信息
     * @param zkPort      zookeepers 端口
     */
    public static void init(String hbaseMaster, String zookeepers, String zkPort) {
        maste = hbaseMaster;
        zks = zookeepers;
        port = zkPort;
    }

    /**
     * 获取HBase Dao实例
     *
     * @return
     */
    public static HBaseDao getInstance() {
        if (instance == null) {
            synchronized (HBaseDao.class) {
                if (instance == null) {
                    instance = new HBaseDao();
                }
            }
        }
        return instance;
    }

    private HBaseDao() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zks);
        configuration.set("hbase.master", maste);
        configuration.set("hbase.zookeeper.property.clientPort", port);
    }


    /**
     * 获取hbase连接
     *
     * @return
     */
    public Connection getConnection() throws IOException {
        if (connection == null || connection.isClosed()) {
            connection = ConnectionFactory.createConnection(configuration);
        }

        return connection;
    }


    public boolean createTable(String name, String columnFamily) {
        try {
            Connection connection = getConnection();
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(name);
            if (admin.tableExists(tableName)) {
                return true;
            }

            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDescriptor);

            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }


    /**
     * 获取一个存储数据文件的表名，此处返回了常量，实际使用可以根据数据库记录信息，动态创建
     * 如：当一张表中存储了100万行后，再创建新表
     *
     * @return
     */
    public String getStoreTable() {
        return "ming_file_table";
    }


    /**
     * 存储文件
     *
     * @param fileMeta
     */
    private static int MAX_STORE_SIZE = 1000;

    public FileMeta saveFile(FileMeta fileMeta) {
        InputStream inputStream = HBaseDao.class.getResourceAsStream(fileMeta.fileName);
        try {
            String storeTable = getStoreTable();
            String columnFamily = "d";
            boolean bret = createTable(storeTable, columnFamily);
            if (!bret) {
                System.out.println("创建存储表失败");
                return fileMeta;
            }

            Connection connection = getConnection();
            if (connection == null) {
                System.out.println("获取hbase连接失败");
                return fileMeta;
            }

            Table table = connection.getTable(TableName.valueOf(storeTable));
            if (table == null) {
                System.out.println("获取表信息失败");
                return fileMeta;
            }
            fileMeta.catalog = storeTable;
            fileMeta.feature = UuidUtil.generate();

            InputStreamReader inputStreamReader = new InputStreamReader(inputStream, fileMeta.encoding);
            CSVParser csvParser = CSVFormat.RFC4180.parse(inputStreamReader);
            Iterator<CSVRecord> records = csvParser.iterator();

            int recordCount = 0;
            int maxColCount = 0;
            boolean skipTitle = false;

            List<Put> putList = new ArrayList<>();

            while (records.hasNext()) {
                if (!skipTitle && fileMeta.hasTitle) {
                    records.next();
                    skipTitle = true;
                    continue;
                }

                CSVRecord csvRecord = records.next();

                Iterator<String> items = csvRecord.iterator();
                int columnCount = 0;
                while (items.hasNext()) {
                    String cellContent = items.next();
                    String qualifier = String.format("f%04d", columnCount);

                    Put put = new Put(Bytes.toBytes(String.format("%s-%08d", fileMeta.feature, recordCount)));
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(cellContent));

                    putList.add(put);
                    if (putList.size() == MAX_STORE_SIZE) {
                        table.put(putList);
                        putList.clear();
                    }

                    columnCount++;
                }

                if (columnCount > maxColCount) {
                    maxColCount = columnCount;
                }

                recordCount++;
            }
            table.put(putList);
            table.close();

            fileMeta.rowCount = recordCount;
            fileMeta.columnCount = maxColCount;

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return fileMeta;
    }

    public void readFile(String catalog, String feature) {
        TableName tableName = TableName.valueOf(catalog);
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(feature));
        scan.setStopRow(Bytes.toBytes(feature + "a"));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        //rowkey正则匹配
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(feature + ".*"));
//        filterList.addFilter(rowFilter);

        //单列前缀
        //ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("f0000"));

        //多列前缀需要加上引号，否则会出错
//        ArrayList<byte[]> columns = new ArrayList<>();
//        columns.add(Bytes.toBytes("'f0001'"));
//        columns.add(Bytes.toBytes("'f0002'"));
//        Filter multipleColumnPrefixFilter = MultipleColumnPrefixFilter.createFilterFromArguments(columns);
//        filterList.addFilter(multipleColumnPrefixFilter);

          //in 条件查询
//        FilterList inCondition = new FilterList(FilterList.Operator.MUST_PASS_ONE);
//        SingleColumnValueFilter singleColumnValueFilter1 = new SingleColumnValueFilter(Bytes.toBytes("d"), Bytes.toBytes("f0001"), CompareFilter.CompareOp.EQUAL,
//                new BinaryComparator(Bytes.toBytes("121.46129743398546")));
//
//        SingleColumnValueFilter singleColumnValueFilter2 = new SingleColumnValueFilter(Bytes.toBytes("d"), Bytes.toBytes("f0001"), CompareFilter.CompareOp.EQUAL,
//                new BinaryComparator(Bytes.toBytes("121.22716296789633")));
//
//        SingleColumnValueFilter singleColumnValueFilter3 = new SingleColumnValueFilter(Bytes.toBytes("d"), Bytes.toBytes("f0001"), CompareFilter.CompareOp.EQUAL,
//                new BinaryComparator(Bytes.toBytes("121.46443656721182")));
//
//        SingleColumnValueFilter singleColumnValueFilter4 = new SingleColumnValueFilter(Bytes.toBytes("d"), Bytes.toBytes("f0001"), CompareFilter.CompareOp.EQUAL,
//                new BinaryComparator(Bytes.toBytes("121.32213164902439")));
//
//        inCondition.addFilter(singleColumnValueFilter1);
//        inCondition.addFilter(singleColumnValueFilter2);
//        inCondition.addFilter(singleColumnValueFilter3);
//        inCondition.addFilter(singleColumnValueFilter4);
//        filterList.addFilter(inCondition);

        // 分页过滤
        PageFilter pageFilter = new PageFilter(100);
        filterList.addFilter(pageFilter);

        scan.setFilter(filterList);
        try {
            Connection connection = getConnection();
            if (connection == null) {
                System.out.println("获取连接失败");
                return;
            }

            Table table = connection.getTable(tableName);
            ResultScanner resultScanner = table.getScanner(scan);

            for (Result r : resultScanner) {
                output(r.listCells());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void output(List<Cell> listCells) {
        boolean cloneMeta = false;
        String rowKey = "";
        String family = "";
        String qualifier = "";
        List<String> contents = new ArrayList<>();

        for (Cell cell : listCells) {
            if (!cloneMeta) {
                rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                family = Bytes.toString(CellUtil.cloneFamily(cell));
                qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                cloneMeta = true;
            }

            String value = Bytes.toString(CellUtil.cloneValue(cell));
            contents.add(value);
        }
        System.out.print("rowkey:" + rowKey + "  列族:" + family + " cols:");
        for (String item : contents) {
            System.out.print(item + "  ");
        }
        System.out.println();
    }


    /**
     * 释放hbase连接
     */
    protected void close() throws Throwable {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}
