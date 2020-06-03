package mr.wordcount;

import mr.score.ScoreMain;
import mr.score.ScoreRunner;
import mr.utils.HadoopUtil;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountMain {
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

        WordCountRunner runner = new WordCountRunner();
        int result = ToolRunner.run(HadoopUtil.getConf(), runner, args);
        LOG.info("计算结果：{}",result);
    }
}
