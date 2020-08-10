package utils;

/**
 * 定义常量
 */
public class Constants {
    /** =======================公共模块常量========================= */
    public static final String SECOND_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DAY_FORMAT = "yyyy-MM-dd";

    /** =======================实时计算常量========================= */
    public static final String STORM_TOPIC = "STORM_DATA_TOPIC";
    public static final String FLINK_TOPIC = "FLINK_DATA_TOPIC_";

    /** =======================离线计算常量========================= */
    public static final String MR_TOPIC = "MR_DATA_TOPIC";
    //parquet文件后缀
    public static final String PARQUET_FILE_SUFFIX = "_parquet";
    //parquet文件合并标识
    public static final String PARQUET_MERGE_MARK = "merge-";
    //DDBook的SCHEMA
    public static final String SCHEMA_UUID = "uuid";
    public static final String SCHEMA_AREA = "area";
    public static final String SCHEMA_CHANNEL = "channel";
    public static final String SCHEMA_STANDARD = "standard";
    public static final String SCHEMA_TIME = "time";
    //MR任务在ES中的INDEX
    public static final String MR_INDEX_NAME = "mr_task";

    /** =======================定时器周期常量======================= */
    public static final String CRON_THIRTY_MINUTE = "0 0/30 * * * ?";
    public static final String CRON_FIVE_MINUTE = "0 0/5 * * * ?";
    public static final String CRON_ONE_MINUTE = "0 0/1 * * * ?";
    public static final String CRON_ONE_HOUR = "0 5 0/1 * * ?";
    public static final String CRON_TEN_SECOND = "*/10 * * * * ?";

    /** =======================NETTY服务器常量======================= */
    public static final String HTTP_SERVER_RESPONSE = "this is test response info";

}
