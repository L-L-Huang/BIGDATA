package utils;

import java.util.HashMap;
import java.util.Map;

/**
 * 定义变量
 */
public class Globals {

    /** =======================离线计算变量======================= */
    //定时器任务
    public static Map<Class, String> JOB_CONF = new HashMap<>();
    //hdfs源文件路径
    public static final String MR_SOURCE_PATH = "/offline/source/";
    //hdfs输出文件路径
    public static final String MR_RESULT_PATH = "/offline/result/";
}
