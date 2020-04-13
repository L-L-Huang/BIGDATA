package count;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountTopology {
    public final static Logger LOGGER = LoggerFactory.getLogger(WordCountTopology.class);

    public static void main(String[] args) throws Exception {
        SentenceSpout sentenceSpout = new SentenceSpout();
        SplitSentenceBolt splitSentenceBolt = new SplitSentenceBolt();
        WordCountBolt wordCountBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        //创建一个拓扑
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        //设置Spout，名称为"sentence-spout",并行度为2(也就是线程数)
        //任务数为4(也就是实例数)，默认是1线程，1任务
        topologyBuilder.setSpout("sentence-spout", sentenceSpout, 2).setNumTasks(4);

        //设置Bolt,名称为“split-bolt”，数据来源是名称为“sentence-spout”的Spout
        //ShuffleGrouping:随机选择一个Task来发送，对Task的分配比较均衡
        topologyBuilder.setBolt("split-bolt", splitSentenceBolt, 2).setNumTasks(4).shuffleGrouping("sentence-spout");
        //FieldsGrouping:根据Tuple中Fields来做一致性hash
        //相同的hash值得Tuple被发送到相同得Task
        topologyBuilder.setBolt("count-bolt",wordCountBolt,2).setNumTasks(4).fieldsGrouping("split-bolt",new Fields("word"));
        //GlobalGrouping:所有得Tuple会被发送到某个Bolt中的id最小的那个Task
        //此时不管有多少个Task，只发一个Task
        topologyBuilder.setBolt("report-bolt",reportBolt,2).setNumTasks(4).globalGrouping("count-bolt");

        Config config = new Config();

        if (args.length < 1) {
            //本地提交，第一个参数为定义的拓扑名称，注意注釋pom.xml的<scope>provided</scope>
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count-topology",config,topologyBuilder.createTopology());
        } else {
            //linux集群提交
            StormSubmitter.submitTopology("word-count-topology", config, topologyBuilder.createTopology());
        }
        LOGGER.info("task submit success");
    }
}
