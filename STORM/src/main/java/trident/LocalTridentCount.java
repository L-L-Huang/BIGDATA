package trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.shade.org.apache.commons.collections.MapUtils;
import org.apache.storm.shade.org.apache.commons.io.FileUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 单词计数
 */
public class LocalTridentCount {

    public static class MyBatchSpout implements IBatchSpout {

        Fields fields;
        HashMap<Long, List<List<Object>>> batches = new HashMap<>();

        public MyBatchSpout(Fields fields) {
            this.fields = fields;
        }

        @Override
        public void open(Map conf, TopologyContext context) {
        }

        @Override
        public void emitBatch(long batchId, TridentCollector collector) {
            List<List<Object>> batch = this.batches.get(batchId);
            if (batch == null) {
                batch = new ArrayList<>();
                Collection<File> listFiles = FileUtils.listFiles(new File("F:\\workpalce\\STREAM_COMPUTE\\STORM\\conf"), new String[]{"txt"}, true);
                for (File file : listFiles) {
                    List<String> readLines;
                    try {
                        readLines = FileUtils.readLines(file);
                        for (String line : readLines) {
                            batch.add(new Values(line));
                        }
//                        FileUtils.moveFile(file, new File(file.getAbsolutePath() + System.currentTimeMillis()));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                if (batch.size() > 0) {
                    this.batches.put(batchId, batch);
                }
            }
            for (List<Object> list : batch) {
                collector.emit(list);
            }
        }

        @Override
        public void ack(long batchId) {
            this.batches.remove(batchId);
        }

        @Override
        public void close() {
        }

        @Override
        public Map getComponentConfiguration() {
            Config conf = new Config();
            conf.setMaxTaskParallelism(1);
            return conf;
        }

        @Override
        public Fields getOutputFields() {
            return fields;
        }

    }

    /**
     * 对一行行的数据进行切割成一个个单词
     */
    public static class MySplit extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String line = tuple.getStringByField("lines");
            String[] words = line.split(" ");
            for (String word : words) {
                collector.emit(new Values(word));
            }
        }

    }

    public static class MyWordAgge extends BaseAggregator<Map<String, Integer>> {

        @Override
        public Map<String, Integer> init(Object batchId,
                                         TridentCollector collector) {
            return new HashMap<>();
        }

        @Override
        public void aggregate(Map<String, Integer> val, TridentTuple tuple,
                              TridentCollector collector) {
            String key = tuple.getString(0);
            /*Integer integer = val.get(key);
            if(integer==null){
                integer=0;
            }
            integer++;
            val.put(key, integer);*/
            val.put(key, MapUtils.getInteger(val, key, 0) + 1);
        }

        @Override
        public void complete(Map<String, Integer> val,
                             TridentCollector collector) {
            collector.emit(new Values(val));
        }

    }

    /**
     * 汇总局部的map，并且打印结果
     */
    public static class MyCountPrint extends BaseFunction {

        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            Map<String, Integer> map = (Map<String, Integer>) tuple.get(0);
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                String key = entry.getKey();
                Integer value = entry.getValue();
                Integer integer = hashMap.get(key);
                if (integer == null) {
                    integer = 0;
                }
                hashMap.put(key, integer + value);
            }

            Utils.sleep(1000);
            System.out.println("==================================");
            for (Map.Entry<String, Integer> entry : hashMap.entrySet()) {
                System.out.println(entry);
            }
        }

    }


    public static void main(String[] args) {
        //大体流程:首先设置一个数据源MyBatchSpout,会监控指定目录下文件的变化,当发现有新文件的时候把文件中的数据取出来,
        //然后封装到一个batch中发射出来.就会对tuple中的数据进行处理,把每个tuple中的数据都取出来,然后切割..切割成一个个的单词.
        //单词发射出来之后,会对单词进行分组,会对一批假设有10个tuple,会对这10个tuple分完词之后的单词进行分组, 相同的单词分一块  
        //分完之后聚合 把相同的单词使用同一个聚合器聚合  然后出结果  每个单词出现多少次...
        //进行汇总  先每一批数据局部汇总  最后全局汇总....

        TridentTopology tridentTopology = new TridentTopology();

        tridentTopology.newStream("spoutid", new MyBatchSpout(new Fields("lines")))
                .each(new Fields("lines"), new MySplit(), new Fields("word"))
                .groupBy(new Fields("word"))//用到了分组 对一批tuple中的单词进行分组..
                .aggregate(new Fields("word"), new MyWordAgge(), new Fields("wwwww"))//用到了聚合
                .each(new Fields("wwwww"), new MyCountPrint(), new Fields(""));

        LocalCluster localCluster = new LocalCluster();
        String simpleName = LocalTridentCount.class.getSimpleName();
        localCluster.submitTopology(simpleName, new Config(), tridentTopology.build());
    }
}