package es;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * ES操作工具类
 */
public class ESUtil<T> {
    public final static Logger LOGGER = LoggerFactory.getLogger(ESUtil.class);
    //饿汉式
    public final static ESUtil INSTANCE = new ESUtil();

    /**
     * 异步插入一条文档
     * @param index
     * @throws Exception
     */
    public static void insertAsync(String index, JSONObject data, String id) {
        IndexRequest indexRequest = new IndexRequest(index).id(id);
        indexRequest.source(data.toJSONString(), XContentType.JSON);
        RestHighLevelClient client = null;
        try {
            client = ESPoolUtil.getClient();
            client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ESAsyncListener());
        } catch (Exception e) {
            LOGGER.error("insert es async fail", e);
        } finally {
            ESPoolUtil.returnClient(client);
        }
    }

    /**
     * 同步插入一条文档
     * @param indexRequest
     */
    public static IndexResponse insertSync(IndexRequest indexRequest){
        RestHighLevelClient client = null;
        IndexResponse indexResponse = null;
        try {
            client = ESPoolUtil.getClient();
            indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            LOGGER.error("insert es sync fail", e);
        } finally {
            ESPoolUtil.returnClient(client);
            return indexResponse;
        }
    }

    /**
     * 更新一条文档
     * @param updateRequest
     */
    public static UpdateResponse updateSync(UpdateRequest updateRequest){
        RestHighLevelClient client = null;
        UpdateResponse updateResponse = null;
        try {
            client = ESPoolUtil.getClient();
            updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            LOGGER.error("insert es sync fail", e);
        } finally {
            ESPoolUtil.returnClient(client);
            return updateResponse;
        }
    }

    /**
     * 异步插入多条文档
     * @param requests
     */
    public static void insertBatchAsync(List<IndexRequest> requests){
        BulkRequest bulkRequest = new BulkRequest();
        for (IndexRequest indexRequest : requests) {
            bulkRequest.add(indexRequest);
        }
        RestHighLevelClient client = null;
        try {
            client = ESPoolUtil.getClient();
            client.bulkAsync(bulkRequest,RequestOptions.DEFAULT, new ESAsyncListener());
        } catch (Exception e) {
            LOGGER.error("insert es batch async fail", e);
        } finally {
            ESPoolUtil.returnClient(client);
        }
    }

    /**
     * 构建IndexRequest
     * @param data
     * @param index
     * @param id
     * @return
     */
    public static IndexRequest buildRequest(JSONObject data, String index, String id){
        IndexRequest indexRequest = new IndexRequest(index).id(id);
        indexRequest.source(data.toJSONString(), XContentType.JSON);
        return indexRequest;
    }

    /**
     * 通用查询
     * @param index
     * @param boolQuery
     * @return
     */
    public List<T> boolQuery(String index, BoolQueryBuilder boolQuery, Class<T> clazz) {
        RestHighLevelClient client = null;
        List<T> list = null;
        try {
            client = ESPoolUtil.getClient();
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder searchSourceBuilder =  new SearchSourceBuilder();
            searchSourceBuilder.query(boolQuery);
            searchRequest.source(searchSourceBuilder);
            //查询结果
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            list = new ArrayList<>(searchHits.length);
            for (SearchHit hit : searchHits) {
                list.add(JSONObject.parseObject(hit.getSourceAsString(), clazz));
            }
        } catch (Exception e) {
            LOGGER.error("boolQuery error", e);
        } finally {
            ESPoolUtil.returnClient(client);
        }
        return list == null ? new ArrayList<>() : list;
    }
}
