package count;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;

/**
 * 单词统计，并且实时获取词频前N的发射出去
 */
public class WordCountBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    //存放单词和词频
    private HashMap<String,Integer> counts = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.counts = new HashMap<String,Integer>();
    }

    /**
     * 进行单词计数工作
     * @param tuple
     */
    public void execute(Tuple tuple) {
        //获取发送过来的单词
        String word = tuple.getStringByField("word");
        System.out.println(this + "====" + word);
        Integer count = counts.get(word);
        if (count == null) {
            count = 0;
        }
        count++;
        counts.put(word, count++);
        this.outputCollector.emit(new Values(word, count));
    }

    /**
     * 设置字段名称，对应this.outputCollector.emit(new Values(word,count));
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
}
