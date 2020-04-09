package count;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.Map;

public class SplitSentenceBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    /**
     * Bolt初始化
     * @param map
     * @param topologyContext
     * @param outputCollector
     */
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    /**
     * 接受Tuple数据进行处理
     * @param tuple
     */
    public void execute(Tuple tuple) {
        //获取发送过来的数据
        String sentence = tuple.getStringByField("sentence");
        String[] words = sentence.split(" ");
        for(String word:words){
            this.outputCollector.emit(new Values(word));
        }
    }

    /**
     * 字段声明
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
