package ddbook;

import count.WordCountTopology;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DDBookTopology {
    public final static Logger LOGGER = LoggerFactory.getLogger(WordCountTopology.class);

    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("ddbook-spout", new DDBookSpout(), 2);
        topologyBuilder.setBolt("ddbook-bolt", new DDBookRecommendBolt(), 2).shuffleGrouping("ddbook-spout");

        Config config = new Config();
        if (args.length < 1) {
            //本地提交，第一个参数为定义的拓扑名称，注意注釋pom.xml的<scope>provided</scope>
            LocalCluster cluster = new LocalCluster();
            config.setNumWorkers(2);
            config.setDebug(true);
            cluster.submitTopology("ddbook-recommend-topology", config, topologyBuilder.createTopology());
        } else {
            //linux集群提交
            config.setDebug(false);
            StormSubmitter.submitTopology("ddbook-recommend-topology", config, topologyBuilder.createTopology());
        }
        LOGGER.info("ddbook task submit success");
    }
}
