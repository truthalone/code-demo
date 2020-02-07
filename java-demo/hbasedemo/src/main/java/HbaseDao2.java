import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ming
 * @date 2020/1/30 12:32
 */
public class HbaseDao2 {
    private Configuration configuration;
    private Connection connection;
    private String table = "ming_table";

    public HbaseDao2() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "host51.ehl.com,host52.ehl.com,host53.ehl.com");
        configuration.set("hbase.master", "host51.ehl.com:60010");
    }

    public Connection getConnection() {
        try {
            if (connection == null || connection.isClosed()) {
                connection = ConnectionFactory.createConnection(configuration);
            }

            return connection;
        } catch (Exception ex) {
            return null;
        }
    }

    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 创建表
     */
    public void createTable() {
        try {
            Connection connection = getConnection();
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(table);

            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }

            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor("c"));

            admin.createTable(tableDescriptor);
            admin.close();

            System.out.println("创建表成功");

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    /**
     * 插入单条数据
     */
    public void insert() {
        Table myTable = null;
        try {
            Connection connection = getConnection();
            myTable = connection.getTable(TableName.valueOf(table));

            Put put = new Put(Bytes.toBytes("1001"));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("name"), Bytes.toBytes("小明"));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("age"), Bytes.toBytes(26));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("address"), Bytes.toBytes("山东 青岛"));
            myTable.put(put);

            System.out.println("插入成功");
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (myTable != null) {
                try {
                    myTable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 插入多条数据
     *
     * @param count
     */
    public void insertMulti(int count) {
        if (count <= 0) {
            return;
        }

        List<Put> putList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Put put = new Put(Bytes.toBytes(String.format("%06d", i)));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("name"), Bytes.toBytes("小明-" + i));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("age"), Bytes.toBytes(26));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("baby"), Bytes.toBytes("this is a baby test -" + i));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("address"), Bytes.toBytes("山东 青岛-" + i));

            putList.add(put);
        }

        Connection connection = getConnection();
        try {
            Table myTable = connection.getTable(TableName.valueOf(table));
            myTable.put(putList);
            myTable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("插入成功");

    }

    /**
     * 根据rowKey查询
     */
    public void queryByRowKey() {
        Connection connection = getConnection();
        Table myTable = null;
        try {
            myTable = connection.getTable(TableName.valueOf(table));

            Get get = new Get(Bytes.toBytes("000093"));
            Result result = myTable.get(get);

            List<Cell> cells = result.listCells();
            output(cells);

            myTable.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 查询全部
     * select * from table
     */
    public void queryAll() {
        Connection connection = getConnection();
        Table myTable = null;
        try {
            myTable = connection.getTable(TableName.valueOf(table));

            Scan scan = new Scan();
            ResultScanner rss = myTable.getScanner(scan);
            for (Result r : rss) {
                output(r.listCells());
            }
            rss.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                myTable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 查询指定条件的数据
     * select * from table where col=?
     */
    public void queryFilter() {
        Connection connection = getConnection();
        Table myTable = null;
        try {
            myTable = connection.getTable(TableName.valueOf(table));

            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("age"));
            scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("name"));
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("c"), Bytes.toBytes("name"),
                    CompareFilter.CompareOp.EQUAL, Bytes.toBytes("小明-100"));

            scan.setFilter(filter);

            ResultScanner rss = myTable.getScanner(scan);
            for (Result r : rss) {
                output(r.listCells());
            }
            rss.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                myTable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 分页查询
     * select * from table where col=? limit 1,10
     */
    public void queryFilterAndPage() {
        Connection connection = getConnection();
        Table myTable = null;
        try {
            myTable = connection.getTable(TableName.valueOf(table));

            Scan scan = new Scan();
            SingleColumnValueFilter scvf =
                    new SingleColumnValueFilter(Bytes.toBytes("c"), Bytes.toBytes("name"),
                            CompareFilter.CompareOp.EQUAL, new SubstringComparator("小明"));
            PageFilter pageFilter = new PageFilter(10);
            scan.setFilter(pageFilter);

            ResultScanner rss = myTable.getScanner(scan);
            for (Result r : rss) {
                output(r.listCells());
            }
            rss.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                myTable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public void rowFilter() {
        Connection connection = getConnection();
        Table myTable = null;
        try {
            myTable = connection.getTable(TableName.valueOf(table));
            Scan scan = new Scan();

            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(".99"));
            PageFilter pageFilter = new PageFilter(10);

            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filterList.addFilter(rowFilter);
            filterList.addFilter(pageFilter);

            scan.setFilter(filterList);

            ResultScanner rss = myTable.getScanner(scan);
            for (Result r : rss) {
                output(r.listCells());
            }
            rss.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                myTable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private void output(List<Cell> listCells) {
        for (Cell cell : listCells) {
            String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
            String c = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            if (qualifier.equals("age")) {
                value = String.valueOf(Bytes.toInt(CellUtil.cloneValue(cell)));
            }

            System.out.println(rowKey + "-" + c + "-" + qualifier + "-" + value);
        }
    }

    public static void main(String[] args) {
        HbaseDao2 hbaseDao2 = new HbaseDao2();

        //创建表
//        hbaseDao2.createTable();

        //插入数据
//        hbaseDao2.insert();

//        插入多条数据
       hbaseDao2.insertMulti(100000);

//        hbaseDao2.queryByRowKey();

//        hbaseDao2.queryAll();

//        hbaseDao2.queryFilter();

//        hbaseDao2.queryFilterAndPage();

//        hbaseDao2.rowFilter();

    }


}
