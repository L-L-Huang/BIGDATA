package ddbook;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Constants;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * 消费检索书籍的信息
 */
public class DDBookSpout extends BaseRichSpout {
    public final static Logger LOGGER = LoggerFactory.getLogger(DDBookSpout.class);
    private KafkaConsumer<byte[], byte[]> consumer;
    SpoutOutputCollector collector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoopc:9092");
        props.put("group.id", "GROUP_".concat(Constants.STORM_TOPIC));
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "10000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(Constants.STORM_TOPIC));
    }

    @Override
    public void nextTuple() {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
        StringBuilder builder;
        for(ConsumerRecord<byte[], byte[]> record:records) {
            builder = new StringBuilder(record.topic()).append("_").append(record.partition()).append("_").append(record.offset());
            String msgId = builder.toString();
            byte[] value = record.value();
            collector.emit(new Values(value), msgId);
            LOGGER.info("DDBOOK consume data success, msgId", msgId);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("record"));
    }

    @Override
    public void ack(Object msgId) {
        if(consumer != null){
            consumer.commitAsync();
            LOGGER.info("offset submit success,msgId:{}",msgId);
        }
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
    }
}
