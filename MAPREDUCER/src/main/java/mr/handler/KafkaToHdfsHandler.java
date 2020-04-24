package mr.handler;

import com.alibaba.fastjson.JSONObject;
import es.ESCommons;
import es.ESUtil;
import model.DDBook;
import mr.enums.MrTaskStatus;
import mr.utils.HadoopUtil;
import mr.utils.ParquetUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Constants;
import utils.DateUtil;
import java.io.IOException;
import java.util.*;

/**
 * kafka数据迁移至hdfs
 */
public enum KafkaToHdfsHandler {
    INSTANCE;
    public final static Logger LOGGER = LoggerFactory.getLogger(KafkaToHdfsHandler.class);
    private KafkaConsumer<byte[], byte[]> consumer;

    KafkaToHdfsHandler(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoopc:9092");
        props.put("group.id", "GROUP_".concat(Constants.MR_TOPIC));
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "20000");
        props.put("max.poll.records", "500");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(Constants.MR_TOPIC));
    }

    //kafka转移至hdfs，手动提交offset
    public void run() {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
        if(records == null || records.isEmpty()){
            LOGGER.warn("have no offset to consume !");
            return;
        }
        ParquetWriter<Group> writer = null;
        long timestamp = System.currentTimeMillis();
        String filePath = ParquetUtil.getWriteFilePath(timestamp);
        String inPath = filePath.concat("/").concat(ParquetUtil.getWriteFileName(timestamp));
        try {
            writer = ParquetUtil.getParquetWriter(inPath);
            Map<TopicPartition,OffsetAndMetadata> partitionAndMetadataMap = new HashMap<>();
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(partition);
                for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                    DDBook ddBook = JSONObject.parseObject(record.value(), DDBook.class);
                    ParquetUtil.parquetWriter(writer, ddBook);
                    partitionAndMetadataMap.put(partition, new OffsetAndMetadata(record.offset() + 1));
                    LOGGER.info("kafka to hdfs process success, inPath:{}, partition:{}, offset:{}", inPath, record.partition(), record.offset());
                }
            }
            ParquetUtil.closeParquetWriter(writer);
            int status = createMrTask(timestamp, filePath);
            if(status == 200 || status == 201){
                consumer.commitSync(partitionAndMetadataMap);
                LOGGER.info("create mr task success, inPath=[{}]", inPath);
                return;
            }
            HadoopUtil.del(inPath);
            LOGGER.error("create mr task error, inPath=[{}]", inPath);
        } catch (IOException e) {
            ParquetUtil.closeParquetWriter(writer);
            HadoopUtil.del(inPath);
            LOGGER.error("MR Consumer ddbook process error, inPath=[{}]", inPath, e);
        }
    }

    //mr任务详情写es
    private int createMrTask(long timestamp, String filePath){
        try {
            StringBuilder builder = new StringBuilder(Constants.MR_INDEX_NAME);
            builder.append("_").append(DateUtil.longToStr(Constants.DAY_FORMAT, timestamp)).append(":").append(DateUtil.getHour(timestamp));
            String mrCode = builder.toString();
            IndexRequest indexRequest = ESCommons.mrRequest(Constants.MR_INDEX_NAME, mrCode, filePath, MrTaskStatus.ACTIVE.getCode());
            IndexResponse indexResponse = ESUtil.insertSync(indexRequest);
            return indexResponse.status().getStatus();
        } catch (Exception e) {
            LOGGER.error("create mr task fail", e);
        }
        return 0;
    }
}
