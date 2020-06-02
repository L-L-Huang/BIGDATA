package trans;

import mr.utils.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;

public class HDFSToHBaseMRRunner {
    public static void main(String[] args) throws Exception {
        if(args == null || args.length < 2){
            System.out.println("请传入日志文件路径");
            return;
        }
        //设置日志文件路径
        System.setProperty("log_dir",args[0]);
        PropertyConfigurator.configure(args[1]);
        //创建job任务，指定job名称
        Configuration conf = HadoopUtil.getConf();
        conf.set("hbase.zookeeper.quorum","hadoopb");
        Job job = Job.getInstance(HadoopUtil.getConf(),"hdfs_to_hbase_job");
        //设置job运行主类
        job.setJarByClass(HDFSToHBaseMRRunner.class);
        //设置文件输入路径
        Path inputpath = new Path("/student.txt");
        FileInputFormat.addInputPath(job, inputpath);
        //设置Mapper类,输出键值类型
        job.setMapperClass(ReadHDFSStudentMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        //初始化reducer任务
        TableMapReduceUtil.initTableReducerJob("student", WriteHBaseStudentReducer.class, job);
        job.setNumReduceTasks(1);
        //执行任务
        boolean isSuccess = job.waitForCompletion(true);
        if(!isSuccess){
            throw new IOException("job execute fail");
        }
        System.out.println("job execute success");
    }
}
