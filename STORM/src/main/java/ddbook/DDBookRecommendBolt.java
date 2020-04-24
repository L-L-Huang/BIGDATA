package ddbook;

import com.alibaba.fastjson.JSONArray;
import es.ESPoolUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
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
        RestHighLevelClient client = null;
        try {
            byte[] record = tuple.getBinaryByField("record");
            JSONArray params = JSONArray.parseArray(new String(record, StandardCharsets.UTF_8));
            if (params == null || params.isEmpty()) {
                LOGGER.warn("ddbook query data empty");
                return;
            }
            client = ESPoolUtil.getClient();
            //执行成功回调,提交offset
            collector.ack(tuple);
        } catch (Exception e) {
            LOGGER.error("recommend error", e);
            collector.fail(tuple);
            //TODO 失败信息写入Redis，定义RedisSpout发送信息至本Bolt再次处理
        } finally {
            ESPoolUtil.returnClient(client);
        }
    }

    //假设执行XX推荐算法
    public void recommend(JSONArray params, RestHighLevelClient client) throws IOException {
        SearchRequest searchRequest = new SearchRequest("ddbook");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        int length = params.size();
        for (int i = 0; i < length; i++) {
            //设定.keyword完全匹配，否则分词器会对中文进行拆分
            String bookType = params.getJSONObject(i).getString("bookType");
            String keyword = new StringBuilder("*").append(bookType).append("*").toString();
            boolQuery.must(QueryBuilders.wildcardQuery("bookType.keyword", keyword));
            searchSourceBuilder.query(boolQuery);
            searchRequest.source(searchSourceBuilder);
            //查询结果
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                System.out.println(hit.getSourceAsString());
                LOGGER.info("es query result:{} ",hit.getSourceAsString());
            }

            //TODO 结果发布到redis
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
