package es;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * ES操作工具类
 */
public class ESUtil {
    public final static Logger LOGGER = LoggerFactory.getLogger(ESUtil.class);

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
            LOGGER.error("insert es fail", e);
        } finally {
            ESPoolUtil.returnClient(client);
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
            LOGGER.error("insert es fail", e);
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

}
