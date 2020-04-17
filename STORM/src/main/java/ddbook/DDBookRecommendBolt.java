package ddbook;

import com.alibaba.fastjson.JSONArray;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class DDBookRecommendBolt extends BaseRichBolt {
    public final static Logger LOGGER = LoggerFactory.getLogger(DDBookRecommendBolt.class);
    OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            byte[] record = tuple.getBinaryByField("record");
            JSONArray queryData = JSONArray.parseArray(new String(record, StandardCharsets.UTF_8));
            if(queryData == null || queryData.isEmpty()){
                LOGGER.warn("ddbook query data empty");
                return;
            }
            LOGGER.info("ddbook query data:{}",queryData.toString());

            //假设执行XX推荐算法，推荐结果发布到redis

            //执行成功回调
            collector.ack(tuple);
        } catch (Exception e) {
            LOGGER.error("recommend error",e);
            collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
