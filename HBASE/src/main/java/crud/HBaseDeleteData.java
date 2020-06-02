package crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDeleteData {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoopb");
        Connection conn = ConnectionFactory.createConnection(conf);
        //Table负责与记录相关的操作，如增删改查等
        TableName tableName = TableName.valueOf("t2");
        Table table = conn.getTable(tableName);

        Delete delete  = new Delete(Bytes.toBytes("row1"));
        table.delete(delete);
        table.close();

        System.out.println("delete date successs !!");
    }
}
