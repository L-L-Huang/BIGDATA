package mr.score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 计算各个学生平均分
 */
public class ScoreMain {
    private static Logger LOG = LoggerFactory.getLogger(ScoreMain.class);

    public static void main(String[] args) throws Exception {

        //检查hadoop目录和用户，一般配置后重启生效
//        System.out.println(System.getenv("HADOOP_USER_NAME"));
//        System.out.println(System.getenv("HADOOP_HOME"));
        if(args == null || args.length < 2){
            System.out.println("请传入日志文件路径");
            return;
        }
        System.setProperty("log_dir",args[0]);
        PropertyConfigurator.configure(args[1]);

        Configuration conf = new Configuration();
        //本地测试或者单节点hadoop使用
//        conf.set("fs.defaultFS","hdfs://hadoopa");

        //hadoop高可用集群
        String nameservices = "mycluster";
        String[] namenodesAddr = {"hadoopa:8020","hadoopb:8020"};
        String[] namenodes = {"nn1","nn2"};
        conf.set("fs.defaultFS", "hdfs://" + nameservices);
        conf.set("dfs.nameservices",nameservices);
        conf.set("dfs.ha.namenodes." + nameservices, namenodes[0]+","+namenodes[1]);
        conf.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[0], namenodesAddr[0]);
        conf.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[1], namenodesAddr[1]);
        conf.set("dfs.client.failover.proxy.provider." + nameservices,"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        ScoreRunner runner = new ScoreRunner();
        int result = ToolRunner.run(conf, runner, args);
        LOG.info("计算结果：{}",result);
    }
}
