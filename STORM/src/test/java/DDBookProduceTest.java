import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import es.ESPoolUtil;
import kafka.ProducerClient;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import utils.Constants;
import java.io.IOException;

public class DDBookProduceTest {

    @Test
    public void sendQueryData() throws InterruptedException {
        while(true){
            JSONArray datas = new JSONArray();
            datas.add(createQueryData("大数据", null));
            datas.add(createQueryData("消息中间件", null));
//            datas.add(createQueryData(null, "a0002"));
            ProducerClient.instance.sendMsg(Constants.STORM_TOPIC, datas.toJSONString().getBytes());
//            System.out.println("ddbook query data send kafka success");
            Thread.sleep(10000);
        }
    }

    private JSONObject createQueryData(String bookType, String bookCode) {
        JSONObject data = new JSONObject();
        data.put("bookType", bookType);
        data.put("bookCode", bookCode);
        return data;
    }

    /** 模糊查询 */
    @Test
    public void query_es_like() throws Exception {
        RestHighLevelClient client = ESPoolUtil.getClient();
        SearchRequest searchRequest = new SearchRequest("ddbook");
        SearchSourceBuilder searchSourceBuilder =  new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //设定.keyword完全匹配，否则分词器会对中文进行拆分
        boolQuery.must(QueryBuilders.wildcardQuery("bookType.keyword", "*大数据*"));
        searchSourceBuilder.query(boolQuery);
        searchRequest.source(searchSourceBuilder);
        try {
            //查询结果
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            for(SearchHit hit : searchHits) {
                System.out.println(hit.getSourceAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
