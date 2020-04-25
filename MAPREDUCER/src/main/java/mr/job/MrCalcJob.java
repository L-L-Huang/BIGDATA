package mr.job;

import es.ESCommons;
import model.MrTaskDetail;
import mr.enums.MrTaskStatus;
import mr.utils.ParquetUtil;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

/**
 * MR激活任务提交，时间粒度：小时
 */
public class MrCalcJob implements Job {
    public final static Logger LOGGER = LoggerFactory.getLogger(MrCalcJob.class);
    @Override
    public void execute(JobExecutionContext jobExecutionContext){
        //检索激活的MR任务
        String mrCode = ParquetUtil.getMrTaskCode(System.currentTimeMillis());
        List<MrTaskDetail> mrTaskDetails = ESCommons.findMrTaskByStatus(MrTaskStatus.ACTIVE.getCode(), mrCode);
        mrTaskDetails.forEach(mrTaskDetail -> {
            try {
                //合并分钟粒度Parquet文件
                if(!MrTaskStatus.CALCULATE_WAIT.name().equals(mrTaskDetail.getMrStatus())){
                    ParquetUtil.mergeParquetFile(mrTaskDetail.getMrInPath());
                    int code = ESCommons.updateMrStatus(MrTaskStatus.CALCULATE_WAIT.getCode(), mrTaskDetail.getMrCode());
                    if(code != 200){
                        LOGGER.warn("update mrStatus fail, dir=[{}], mrCode=[{}]", mrTaskDetail.getMrInPath(), mrCode);
                        //foreach中return效果与for循环中continue效果相同
                        return;
                    }
                }
                //提交MR计算任务

            } catch (Exception e) {
                LOGGER.error("merge parquet file fail, dir=[{}]", mrTaskDetail.getMrInPath());
            }
        });
        LOGGER.info("MR CALC job execute success，mr task size=[{}]", mrTaskDetails.size());
    }

}
