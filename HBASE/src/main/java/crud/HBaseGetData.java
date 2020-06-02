package crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseGetData {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoopb");
        Connection conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("t2");
        Table table = conn.getTable(tableName);
        Get get = new Get(Bytes.toBytes("row1"));

        //查询数据，取得结果集
        Result r = table.get(get);
        for(Cell cell : r.rawCells()){
            //取得当前单元格所属的列族名称
            String family = new String(CellUtil.cloneFamily(cell));
            //取得当前单元格所属的列名称
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            //取得当前单元格的值
            String value = new String(CellUtil.cloneValue(cell));
            System.out.println("列：" + family + ":" + qualifier + "----值："+ value);
        }
    }
}
