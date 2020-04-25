package mr.utils;

import model.DDBook;
import mr.handler.KafkaToHdfsHandler;
import org.apache.hadoop.fs.*;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Constants;
import utils.DateUtil;
import utils.Globals;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parquet操作工具
 */
public class ParquetUtil {
    public final static Logger LOGGER = LoggerFactory.getLogger(KafkaToHdfsHandler.class);
    private static MessageType schema = null;

    static{
        initDDbookSchema();
    }

    //入hdfs
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

    //hdfs目录
    public static String getWriteFileDir(long timestamp){
        //构建HDFS Parquet文件全路径
        String dayFormat = DateUtil.longToStr(Constants.DAY_FORMAT, timestamp);
        int hour = DateUtil.getHour(timestamp);
        StringBuilder inPath = new StringBuilder(Globals.MR_SOURCE_PATH);
        inPath.append(dayFormat).append("/").append(hour);
        return inPath.toString();
    }

    //hdfs文件名称
    public static String getWriteFileName(long timestamp){
        int minute = DateUtil.getMinute(timestamp);
        StringBuilder fileName = new StringBuilder().append(minute).append(Constants.PARQUET_FILE_SUFFIX);
        return fileName.toString();
    }

    //parquet读取工具
    public static ParquetReader getParquetReader(Path path) throws IOException {
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader.Builder<Group> builder = ParquetReader.builder(readSupport, path);
        builder.withConf(HadoopUtil.getConf());
        ParquetReader<Group> parquetReader = builder.build();
        return parquetReader;
    }

    //parquet写入工具
    public static ParquetWriter<Group> getParquetWriter(Path path) throws IOException {
        ExampleParquetWriter.Builder builder = ExampleParquetWriter
                .builder(path).withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withConf(HadoopUtil.getConf())
                .withType(schema);
        ParquetWriter<Group> writer = builder.build();
        return writer;
    }

    //合并指定目录下parquet文件
    public static void mergeParquetFile(String dir) throws Exception {
        FileSystem fileSystem = FileSystem.get(HadoopUtil.getConf());
        Path dirPath = new Path(dir);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(dirPath, false);
        List<Path> parquetPaths = new ArrayList<>();
        //整理需要合并的parquet文件路径
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            Path path = next.getPath();
            FileStatus fileStatus = fileSystem.getFileStatus(path);
            String fileName = path.getName();
            if(fileName.contains(Constants.PARQUET_MERGE_MARK)){
                LOGGER.info("parquet file has been merged, fileName=[{}]", fileName);
                return;
            }
            //parquet后缀文件添加
            if(fileStatus.isFile() && fileName.endsWith(Constants.PARQUET_FILE_SUFFIX)) {
                parquetPaths.add(path);
            }
        }

        //TODO 如果status=2 && 已包含merge文件，则属于删除操作未执行完毕
        //TODO 如果statue=2 && 不包含merge文件，则属于合并操作未执行完毕

        String mergeDir = new StringBuilder(dir).append("/").append(Constants.PARQUET_MERGE_MARK).append(System.currentTimeMillis()).
                append(Constants.PARQUET_FILE_SUFFIX).toString();
        Path mergePath = new Path(mergeDir);
        ParquetReader<Group> parquetReader = null;
        ParquetWriter<Group> parquetWriter = getParquetWriter(mergePath);
        Group book;
        //TODO 如果业务扩展，则根据schama分组，这里我们就直接合并
        //开始合并parquet文件
        for(Path path : parquetPaths) {
            parquetReader = getParquetReader(path);
            while ((book = parquetReader.read()) != null) {
                parquetWriter.write(book);
            }
        }
        closeParquetReader(parquetReader);
        closeParquetWriter(parquetWriter);
        //开始清除源parquet文件
        if(fileSystem.exists(mergePath)){
            for (Path path : parquetPaths) {
                fileSystem.delete(path, true);
            }
        }
        HadoopUtil.closeFileSystem(fileSystem);
        LOGGER.info("merge parquet file success, dir=[{}]", dir);
    }

    public static String getMrTaskCode(long timestamp){
        StringBuilder builder = new StringBuilder(Constants.MR_INDEX_NAME);
        builder.append("_").append(DateUtil.longToStr(Constants.DAY_FORMAT, timestamp)).append(":").append(DateUtil.getHour(timestamp));
        return builder.toString();
    }

    //关闭读取工具
    public static void closeParquetReader(ParquetReader<Group> reader) {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                LOGGER.error("close ParquetReader error!", e);
            }
        }
    }

    //关闭写入工具
    public static void closeParquetWriter(ParquetWriter<Group> writer) {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                LOGGER.error("close ParquetWriter error!", e);
            }
        }
    }
}
