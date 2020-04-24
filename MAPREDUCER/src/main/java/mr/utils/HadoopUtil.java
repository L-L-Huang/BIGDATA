package mr.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class HadoopUtil {
    private static Logger LOGGER = LoggerFactory.getLogger(HadoopUtil.class);
    //HADOOP集群配置
    private static Configuration HADOOP_CONF;

    static{
        HADOOP_CONF = new Configuration();
        //本地测试或者单节点hadoop使用
//        conf.set("fs.defaultFS","hdfs://hadoopa");

        //hadoop高可用集群,本地启动默认连接103(mycluster)，linux提交则处于ha模式
        String nameservices = "mycluster";
        String[] namenodesAddr = {"hadoopa:8020","hadoopb:8020"};
        String[] namenodes = {"nn1","nn2"};
        HADOOP_CONF.set("fs.defaultFS", "hdfs://" + nameservices);
        HADOOP_CONF.set("dfs.nameservices",nameservices);
        HADOOP_CONF.set("dfs.ha.namenodes." + nameservices, namenodes[0]+","+namenodes[1]);
        HADOOP_CONF.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[0], namenodesAddr[0]);
        HADOOP_CONF.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[1], namenodesAddr[1]);
        HADOOP_CONF.set("dfs.client.failover.proxy.provider." + nameservices,"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    }

    public static Configuration getConf(){
        return HADOOP_CONF;
    }

    /**
     * 删除
     *
     * @param path
     * @throws
     */
    public static void del(String path) {
        if (StringUtils.isEmpty(path)) {
            return;
        }
        FileSystem fileSystem = null;
        String tmp = null;
        try {
            fileSystem = FileSystem.get(HadoopUtil.getConf());
            boolean deleted = fileSystem.delete(new Path(path), true);
            LOGGER.info("hdfs file deleted=[{}], path=[{}]", deleted, path);
        } catch (Exception e) {
            LOGGER.error("hadoop util. error to del path! path:{}", tmp, e);
        } finally {
            HadoopUtil.closeFileSystem(fileSystem);
        }
    }

    /**
     * 关闭资源
     *
     * @param fileSystem
     */
    public static void closeFileSystem(FileSystem fileSystem) {
        if (fileSystem != null) {
            try {
                fileSystem.close();
            } catch (IOException e) {
                LOGGER.error("close fileSystem error!", e);
            }
        }
    }
}
