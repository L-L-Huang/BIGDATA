package mr.job;

import mr.handler.KafkaToHdfsHandler;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafka消费定时器
 */
public class KafkaConsumerJob implements Job {
    public final static Logger LOGGER = LoggerFactory.getLogger(KafkaToHdfsHandler.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        KafkaToHdfsHandler.INSTANCE.run();
        LOGGER.info("kafka consumer job execute success");
    }
}
