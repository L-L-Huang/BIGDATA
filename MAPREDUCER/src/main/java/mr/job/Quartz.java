package mr.job;

import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Globals;
import java.util.Map;
import java.util.Properties;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 * 定时任务
 */
public enum Quartz {
    INSTANCE;
    private final static Logger LOGGER = LoggerFactory.getLogger(Quartz.class);

    public void run() throws Exception {
        StdSchedulerFactory ssf = new StdSchedulerFactory();
        Properties props = new Properties();
        props.put(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
        props.put(StdSchedulerFactory.PROP_SCHED_SKIP_UPDATE_CHECK, "true");
        props.put("org.quartz.threadPool.threadCount", String.valueOf(Globals.JOB_CONF.size()));
        //初始化调度器
        ssf.initialize(props);
        Scheduler sched = ssf.getScheduler();
        for (Map.Entry<Class, String> entry : Globals.JOB_CONF.entrySet()) {
            Class clazz = entry.getKey();
            String cron = entry.getValue();
            try {
                // 定时作业
                JobDetail jobDetail = newJob(clazz)
                        .withIdentity(clazz.getSimpleName(), Scheduler.DEFAULT_GROUP)
                        .build();
                // 定时觖发器
                CronTrigger cronTrigger = newTrigger()
                        .withIdentity(clazz.getSimpleName(), Scheduler.DEFAULT_GROUP)
                        .withSchedule(cronSchedule(cron))
                        .build();
                sched.scheduleJob(jobDetail, cronTrigger);
                LOGGER.info("add startup the quartz scheduler, cron name:{}, cron:{}", clazz.getSimpleName(), cron);
            } catch (Exception e) {
                LOGGER.error("quartz scheduler startup error!", e);
            }
        }
        sched.start();
    }
}
