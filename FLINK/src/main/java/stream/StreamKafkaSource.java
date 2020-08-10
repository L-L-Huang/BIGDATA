package stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import utils.Constants;
import java.util.Properties;

public class StreamKafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoopc:9092");
        props.put("group.id", "GROUP_".concat(Constants.FLINK_TOPIC));
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(Constants.FLINK_TOPIC,new SimpleStringSchema(),props);
        consumer.setStartFromGroupOffsets();
        DataStreamSource<String> text = env.addSource(consumer);
        DataStream<WordWithCount> wordcounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] arr = value.split("[,\\s]");
                for(String word:arr){
                    out.collect(new WordWithCount(word, 1L));
                }
            }
        }).keyBy("word")
                .timeWindow(Time.seconds(2),Time.seconds(1))//指定时间窗口大小为2s,指定时间间隔为1s
                .sum("count");//在这里使用sum或者reduce都可以

        wordcounts.print().setParallelism(1);
//        text.print().setParallelism(1);
        env.execute("word count");

    }
}
