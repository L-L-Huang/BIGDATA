package mr.ddbook;

import mr.job.KafkaConsumerJob;
import mr.job.MrCalcJob;
import mr.job.Quartz;
import mr.score.ScoreMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Constants;
import utils.Globals;

/**
 * 离线计算启动
 */
public class DDBookMain {
    private static Logger LOGGER = LoggerFactory.getLogger(ScoreMain.class);

    public static void main(String[] args) {
        try {
            //添加定时任务
            Globals.JOB_CONF.put(KafkaConsumerJob.class, Constants.CRON_FIVE_MINUTE);
            Globals.JOB_CONF.put(MrCalcJob.class, Constants.CRON_ONE_HOUR);
            //启动定时任务
            Quartz.INSTANCE.run();
            LOGGER.info("DDBook MR Server started");
        } catch (Exception e) {
            LOGGER.error("startup error", e);
            System.exit(0);
        }
    }
}
