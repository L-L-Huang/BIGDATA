package string;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * kafka生产者
 */
public class ProducerClientString {
    private Logger logger = LoggerFactory.getLogger(ProducerClientString.class);
    public static ProducerClientString instance = new ProducerClientString();
    private KafkaProducer<String, String> producer;

    public ProducerClientString(){
        initKafkaParams();
    }

    private void initKafkaParams(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoopc:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 335544320);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 335544320);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put (ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 163840);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    public void sendMsg(String topic, String msg) {
        producer.send(new ProducerRecord<>(topic, msg), new KafkaCallBack());
    }

    private class KafkaCallBack implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                logger.error("send error", e);
                return;
            }
            logger.info("send success, topic : {}, partition : {}, offset: {} ", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
        }
    }
}
