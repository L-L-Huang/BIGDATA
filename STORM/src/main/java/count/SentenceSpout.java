package count;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import java.util.Map;
import java.util.Random;

/**
 * 消息源Spout，随机选取一句英文作为源数据，发射出去
 */
public class SentenceSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    Random random;
    private String[] sentences = {
            "she didn’t know how she was going to make it",
            "It seemed just as one problem was solved another one soon followed",
            "He then let them sit and boil without saying a word to his daughter",
            "He took the potatoes out of the pot and placed them in a bowl",
            "the eggs and coffee beans had each faced the same adversity the boiling water",
            "However the ground coffee beans were unique After they were exposed to the boiling water they changed the water and created something new",
            "He filled three pots with water and placed each on a high fire",
            "eggs in the second pot and ground coffee beans in the third pot"
    };

    /**
     * 初始化
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        random = new Random();
    }

    /**
     * 每2s发射一次数据
     */
    public void nextTuple() {
        Utils.sleep(2000);
        int randomNum = random.nextInt(sentences.length);
        String sentence = sentences[randomNum];
        this.spoutOutputCollector.emit(new Values(sentence));
    }

    /**
     * 定义字段名称，对应emit(new Values(sentence)中的字段
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));

    }
}
