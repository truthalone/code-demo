import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ming
 * @date 2019/12/17 11:55
 */
public class HbaseDao {
    private Configuration configuration;

    public HbaseDao() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "host51.ehl.com,host52.ehl.com,host53.ehl.com");
        configuration.set("hbase.master", "host51.ehl.com:60010");
    }

    /**
     * 创建表
     *
     * @param tableName
     */
    public void createTable(String tableName) {
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);

            Admin hBaseAdmin = connection.getAdmin();
            TableName name = TableName.valueOf(tableName);
            if (hBaseAdmin.tableExists(name)) {
                hBaseAdmin.disableTable(name);
                hBaseAdmin.deleteTable(name);
            }

            HTableDescriptor hTableDescriptor = new HTableDescriptor(name);
            hTableDescriptor.addFamily(new HColumnDescriptor("c1"));
            hTableDescriptor.addFamily(new HColumnDescriptor("c2"));

            hBaseAdmin.createTable(hTableDescriptor);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    public void insertData(String tableName) {
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(tableName));

            Put put = new Put("100".getBytes());
            put.addColumn("c1".getBytes(), "name".getBytes(), "小明".getBytes());
            put.addColumn("c2".getBytes(), "age".getBytes(), "20".getBytes());
            table.put(put);

            table.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void queryByRowKey(String rowKey, String tableName) {
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(tableName));

            Get get = new Get("100".getBytes());
            Result result = table.get(get);

            List<Cell> cellList = result.listCells();
            for (Cell cell : cellList) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qu = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println("row:" + row + " family:" + family + " qu:" + qu + " value:" + value);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void delete(String tableName, String rowKey) {
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(tableName));

            Delete delete = new Delete(rowKey.getBytes());
            table.delete(delete);

            table.close();
            connection.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void deleteTable(String tableName) {
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            admin.deleteTable(TableName.valueOf(tableName));

            admin.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    public void queryCameraPath() {
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf("camera_path"));

            String rowKey = "18026P0009";
            Get get = new Get(Bytes.toBytes(rowKey));

            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println(row + "--" + qualifier + "--" + value);
            }

            table.close();
            connection.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    public void scanCameraPath() {
        try{
            Scan scan = new Scan();

        }catch (Exception ex){
            ex.printStackTrace();
        }
    }


    public void queryMultiCameraPath() {
        String[] rowKeys = new String[]{"nokey", "18026P0009", "1802621003", "1802621002", "1802618106", "1802618023", "1802617104"};

        List<Get> getList = new ArrayList<>();
        for (String rowKey : rowKeys) {
            Get get = new Get(Bytes.toBytes(rowKey));
            getList.add(get);
        }
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf("camera_path"));

            Result[] results = table.get(getList);
            for (Result result : results) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));

                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    if (qualifier.equals("slon") || qualifier.equals("slat") || qualifier.equals("elon") || qualifier.equals("elat")) {
                        value = String.valueOf(Bytes.toDouble(CellUtil.cloneValue(cell)));
                    }

                    System.out.println(rowKey + "--" + qualifier + "--" + value);
                }
            }
            table.close();
            connection.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
