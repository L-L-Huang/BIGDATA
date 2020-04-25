package es;

import com.alibaba.fastjson.JSONObject;
import model.MrTaskDetail;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import utils.Constants;
import java.util.List;

/**
 * ES相关公共业务方法
 */
public class ESCommons {

    /**
     * 插入
     * @param index
     * @param objs
     * @return
     */
    public static IndexRequest insertRequest(String index, Object... objs) {
        JSONObject data = new JSONObject();
        data.put("mrCode",objs[0]);
        data.put("mrInPath",objs[1]);
        data.put("mrStatus",objs[2]);
        IndexRequest indexRequest = ESUtil.buildRequest(data, index, objs[0].toString());
        return indexRequest;
    }

    /**
     * 检索
     * @param status
     * @param mrCode
     * @return
     */
    public static List<MrTaskDetail> findMrTaskByStatus(int status,String mrCode){
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.termQuery("mrStatus", status));
        //特殊字符添加.keyword全量匹配
        boolQuery.mustNot(QueryBuilders.matchQuery("mrCode.keyword", mrCode));
        return ESUtil.INSTANCE.boolQuery(Constants.MR_INDEX_NAME, boolQuery, MrTaskDetail.class);
    }

    /**
     * 更新
     * @param status
     * @param id
     * @return
     */
    public static int updateMrStatus(int status, String id){
        JSONObject data = new JSONObject();
        data.put("mrStatus",status);
        UpdateRequest request = new UpdateRequest(Constants.MR_INDEX_NAME, id);
        request.doc(data.toJSONString(), XContentType.JSON);
        UpdateResponse response = ESUtil.updateSync(request);
        return response.status().getStatus();
    }
}
