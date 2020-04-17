package elasticsearch;

import com.alibaba.fastjson.JSONObject;
import es.ESPoolUtil;
import es.ESUtil;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ES图书信息初始化
 */
public class DDBookTest {

    @Test
    public void initBooks() throws InterruptedException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(createRequest("ddbook",null,"a0001","消息中间件","Apache kafka实战"));
        requests.add(createRequest("ddbook",null,"a0002","消息中间件","RabbitMQ实战指南"));
        requests.add(createRequest("ddbook",null,"b0001","大数据组件","Druid实时大数据分析原理与实践"));
        requests.add(createRequest("ddbook",null,"b0002","大数据组件","Elasticsearch技术解析与实战"));
        requests.add(createRequest("ddbook",null,"b0003","大数据组件","Hadoop权威指南 大数据的存储与分析"));
        requests.add(createRequest("ddbook",null,"b0004","大数据组件","Elasticsearch源码解析与优化实践"));
        requests.add(createRequest("ddbook",null,"b0005","大数据组件","Storm分布式实时计算模式"));
        requests.add(createRequest("ddbook",null,"b0006","大数据组件","Hadoop大数据技术开发实战"));
        requests.add(createRequest("ddbook",null,"c0001","基础组件","Nginx高性能Web服务器详解"));
        requests.add(createRequest("ddbook",null,"c0002","基础组件","Redis开发与运维"));
        requests.add(createRequest("ddbook",null,"d0001","基础架构","架构解密从分布式到微服务Leaderus著"));
        requests.add(createRequest("ddbook",null,"d0002","基础架构","深入分布式缓存 从原理到实践"));
        requests.add(createRequest("ddbook",null,"e0001","Java","Netty权威指南 第2版"));
        requests.add(createRequest("ddbook",null,"e0002","Java","实战Java高并发程序设计"));
        requests.add(createRequest("ddbook",null,"e0003","Java","实战JAVA虚拟机 JVM故障诊断与性能优化"));
        requests.add(createRequest("ddbook",null,"e0004","Java","SpringBoot 实战"));
        requests.add(createRequest("ddbook",null,"f0001","算法","零基础学大数据算法"));
        requests.add(createRequest("ddbook",null,"f0002","算法","算法图解-袁国忠"));
        requests.add(createRequest("ddbook",null,"f0003","算法","算法第四版-谢路云"));
        ESUtil.insertBatchAsync(requests);
        Thread.sleep(5000);
    }

    public IndexRequest createRequest(String index, String id, String... objs) {
        JSONObject data = new JSONObject();
        data.put("bookCode",objs[0]);
        data.put("bookType",objs[1]);
        data.put("bookName",objs[2]);
        IndexRequest indexRequest = ESUtil.buildRequest(data, index, id);
        return indexRequest;
    }

    /** 指定ID删除文档 */
    @Test
    public void delete() throws Exception {
        RestHighLevelClient client = ESPoolUtil.getClient();
        DeleteRequest request = new DeleteRequest("ddbook").id("1");
        DeleteResponse response = client.delete(request,RequestOptions.DEFAULT);
        System.out.println(response.getResult());
    }

    @Test
    public void query() throws Exception {
        RestHighLevelClient client = ESPoolUtil.getClient();
        SearchRequest searchRequest = new SearchRequest("ddbook");
        SearchSourceBuilder searchSourceBuilder =  new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
//        boolQuery.must(QueryBuilders.wildcardQuery("host", "10.229.208.*"));
//        boolQuery.mustNot(QueryBuilders.matchQuery("message", "DISPLAY_CMDRECORD"));
//        boolQuery.mustNot(QueryBuilders.matchQuery("message", "SUPPRESS_LOG"));
//        boolQuery.filter(QueryBuilders.rangeQuery("@timestamp").gte(start).lte(end));
        searchSourceBuilder.query(boolQuery);
        String[] includeFields = new String[] {"message", "@timestamp"};
        String[] excludeFields = new String[] {};
        searchSourceBuilder.fetchSource(includeFields, excludeFields);
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
