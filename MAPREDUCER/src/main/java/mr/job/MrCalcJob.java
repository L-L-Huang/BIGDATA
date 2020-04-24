package mr.job;

import es.ESCommons;
import model.MrTaskDetail;
import mr.enums.MrTaskStatus;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import java.util.List;

/**
 * MR激活任务提交，时间粒度：小时
 */
public class MrCalcJob implements Job {
    @Override
    public void execute(JobExecutionContext jobExecutionContext){
        //检索激活的MR任务
        List<MrTaskDetail> mrTaskDetails = ESCommons.findMrTaskByStatus(MrTaskStatus.ACTIVE.getCode());
        mrTaskDetails.forEach(mrTaskDetail -> {
            //合并分钟级别Parquet文件

        });


    }
}
