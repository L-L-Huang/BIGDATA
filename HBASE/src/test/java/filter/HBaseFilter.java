package filter;//package filter;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.*;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.filter.*;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.junit.Before;
//import org.junit.Test;
//import java.io.IOException;
//
///**
// * HBase 过滤器
// */
//public class HBaseFilter {
//    private Table table;
//
//    @Before
//    public void init() throws IOException {
//        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum","hadoopb");
//        Connection conn = ConnectionFactory.createConnection(conf);
//
//        TableName tableName = TableName.valueOf("t2");
//        table = conn.getTable(tableName);
//    }
//
//    //行键过滤器
//    @Test
//    public void rowKeyFilter() throws IOException {
//        //创建Scan对象
//        Scan scan = new Scan();
//        Filter filter = new RowFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("row1")));
//        scan.setFilter(filter);
//        //查询数据，返回结果集
//        ResultScanner rs = table.getScanner(scan);
//        for(Result res: rs){
//            System.out.println(res);
//        }
//    }
//
//    //列族过滤器
//    @Test
//    public void familyFilter() throws IOException {
//        //创建Scan对象
//        Scan scan = new Scan();
//        Filter filter = new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("f2")));
//        scan.setFilter(filter);
//        //查询数据，返回结果集
//        ResultScanner rs = table.getScanner(scan);
//        for(Result res: rs){
//            System.out.println(res);
//        }
//    }
//
//    //列限定符过滤器
//    @Test
//    public void qualifierFilter() throws IOException {
//        //创建Scan对象
//        Scan scan = new Scan();
//        Filter filter = new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("name")));
//        scan.setFilter(filter);
//        //查询数据，返回结果集
//        ResultScanner rs = table.getScanner(scan);
//        for(Result res: rs){
//            System.out.println(res);
//        }
//    }
//
//    //值过滤器
//    @Test
//    public void valueFilter() throws IOException {
////        //创建Scan对象
////        Scan scan = new Scan();
////        Filter filter = new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("xiaoming"));
////        scan.setFilter(filter);
////        //查询数据，返回结果集
////        ResultScanner rs = table.getScanner(scan);
////        for(Result res: rs){
////            System.out.println(res);
////        }
//
//        //查询name不等于小明的全部数据
//        Scan scan = new Scan();
//        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("f2"), Bytes.toBytes("name"),
//                CompareOperator.NOT_EQUAL, new SubstringComparator("xiaoming"));
//        //如果某行name列不存在，那么改行将被过滤掉，false则不进行过滤，默认为false
//        ((SingleColumnValueFilter) filter).setFilterIfMissing(true);
//        scan.setFilter(filter);
//        //查询数据，返回结果集
//        ResultScanner rs = table.getScanner(scan);
//        for(Result res: rs){
//            System.out.println(res);
//        }
//    }
//
//    //多条件过滤
//    @Test
//    public void muchFilter() throws IOException {
//        Scan scan = new Scan();
//        Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("f2"), Bytes.toBytes("age"),
//                CompareOperator.LESS_OR_EQUAL, Bytes.toBytes("30"));
//        Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("f2"), Bytes.toBytes("age"),
//                CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes("21"));
//
//        FilterList filterList = new FilterList();
//        filterList.addFilter(filter1);
//        filterList.addFilter(filter2);
//        scan.setFilter(filterList);
//
//        ResultScanner rs = table.getScanner(scan);
//        for(Result res: rs){
//            System.out.println(res);
//        }
//    }
//}
