package mr.utils;

import model.DDBook;
import mr.handler.KafkaToHdfsHandler;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Constants;
import utils.DateUtil;
import utils.Globals;
import java.io.IOException;

/**
 * Parquet操作工具
 */
public class ParquetUtil {
    public final static Logger LOGGER = LoggerFactory.getLogger(KafkaToHdfsHandler.class);
    private static MessageType schema = null;

    static{
        initDDbookSchema();
    }

    public static void parquetWriter(ParquetWriter<Group> writer, DDBook ddBook) throws IOException {
        writer.write(getGroup(ddBook));
    }

    public static Group getGroup(DDBook ddBook){
        GroupFactory factory = new SimpleGroupFactory(schema);
        Group group = factory.newGroup();
        group.add(Constants.SCHEMA_UUID, ddBook.getUuid());
        group.add(Constants.SCHEMA_AREA, ddBook.getArea());
        group.add(Constants.SCHEMA_CHANNEL, ddBook.getChannel());
        group.add(Constants.SCHEMA_STANDARD, ddBook.getStandard());
        group.add(Constants.SCHEMA_TIME, ddBook.getTime());
        return group;
    }

    private static void initDDbookSchema() {
        schema = MessageTypeParser.parseMessageType("message Pair {\n" +
                " required binary uuid;\n" +
                " required binary area (UTF8);\n" +
                " required binary channel (UTF8);\n" +
                " required binary standard (UTF8);\n" +
                " required int64  time;\n" +
                "}\n" +
                "}");
    }

    /**
     * hdfs文件路径
     * @param timestamp
     * @return
     */
    public static String getWriteFilePath(long timestamp){
        //构建HDFS Parquet文件全路径
        String dayFormat = DateUtil.longToStr(Constants.DAY_FORMAT, timestamp);
        int hour = DateUtil.getHour(timestamp);
        StringBuilder inPath = new StringBuilder(Globals.MR_SOURCE_PATH);
        inPath.append(dayFormat).append("/").append(hour);
        return inPath.toString();
    }

    /**
     * hdfs文件名称
     * @param timestamp
     * @return
     */
    public static String getWriteFileName(long timestamp){
        int minute = DateUtil.getMinute(timestamp);
        StringBuilder fileName = new StringBuilder().append(minute).append(Constants.PARQUET_FILE_SUFFIX);
        return fileName.toString();
    }

    public static ParquetWriter<Group> getParquetWriter(String inPath) throws IOException {
        Path path = new Path(inPath);
        ExampleParquetWriter.Builder builder = ExampleParquetWriter
                .builder(path).withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withConf(HadoopUtil.getConf())
                .withType(schema);
        ParquetWriter<Group> writer = builder.build();
        return writer;
    }

    /**
     * 关闭资源
     *
     * @param writer
     */
    public static void closeParquetWriter(ParquetWriter<Group> writer) {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                LOGGER.error("close fileSystem error!", e);
            }
        }
    }

}
