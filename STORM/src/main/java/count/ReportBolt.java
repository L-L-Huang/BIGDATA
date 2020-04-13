package count;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class ReportBolt extends BaseRichBolt {
    private HashMap<String,Integer> counts = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counts = new HashMap();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        int count = tuple.getIntegerByField("count");
        counts.put(word, count);
        //对counts中的单词进行排序
        List<Map.Entry<String, Integer>> list = new ArrayList(counts.entrySet());
    //        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
    //            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
    //                return o2.getValue() - o1.getValue();
    //            }
    //        });

        Collections.sort(list, (a, b) -> b.getValue().compareTo(a.getValue()));
        //取list中前10个单词
        int n = list.size() <= 10 ? list.size() : 10;
        String resultStr = "";
        for (int i = 0; i < n; i++) {
            Map.Entry<String, Integer> entry = list.get(i);
            String sortWord = entry.getKey();
            Integer sortCount = entry.getValue();
            resultStr += sortWord + "====" + sortCount + "\n";
        }
        System.out.println("==============计数结果===============");
        //添加这行代码的作用是看看是不是同一个实例执行的
        System.out.println(this + "====" + word);
        System.out.println(resultStr);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
