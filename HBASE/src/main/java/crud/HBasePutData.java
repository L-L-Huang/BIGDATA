package crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBasePutData {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoopb");
        Connection conn = ConnectionFactory.createConnection(conf);
        //Table负责与记录相关的操作，如增删改查等
        TableName tableName = TableName.valueOf("t2");
        Table table = conn.getTable(tableName);

//        Put put = new Put(Bytes.toBytes("row1"));//设置rowkey
//        put.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("name"),Bytes.toBytes("xiaoming"));
//        put.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("age"),Bytes.toBytes("20"));
//        put.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("address"),Bytes.toBytes("chongqing"));

        Put put1 = new Put(Bytes.toBytes("row2"));//设置rowkey
        put1.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("name"),Bytes.toBytes("xiaohong"));
        put1.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("age"),Bytes.toBytes("20"));
        put1.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("address"),Bytes.toBytes("beijing"));

        Put put2 = new Put(Bytes.toBytes("row3"));//设置rowkey
        put2.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("name"),Bytes.toBytes("张三"));
        put2.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("age"),Bytes.toBytes("18"));
        put2.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("address"),Bytes.toBytes("shanghai"));

        Put put3 = new Put(Bytes.toBytes("row4"));//设置rowkey
        put3.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("name"),Bytes.toBytes("李四"));
        put3.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("age"),Bytes.toBytes("30"));
        put3.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("address"),Bytes.toBytes("shanghai"));

//        table.put(put);
        table.put(put1);
        table.put(put2);
        table.put(put3);
        table.close();

        System.out.println("put date successs !!");
    }
}
