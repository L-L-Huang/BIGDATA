package crud;//package crud;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//import java.io.IOException;
//
//public class HBaseCreateTeble {
//    public static void main(String[] args) throws IOException {
//        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum","hadoopb");
//        Connection conn = ConnectionFactory.createConnection(conf);
//        Admin admin = conn.getAdmin();
//        //创建表描述
//        TableName tableName = TableName.valueOf("t2");
//        TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
//        //创建族列描述
//        String family = "f2";
//        ColumnFamilyDescriptorBuilder columnDescBuilder = ColumnFamilyDescriptorBuilder
//                .newBuilder(Bytes.toBytes(family));
//        //指定族列
//        tableDescriptor.setColumnFamily(columnDescBuilder.build());
//        //创建表
//        admin.createTable(tableDescriptor.build());
//        System.out.println("create table success !!");
//    }
//}
