package es;

import com.alibaba.fastjson.JSONObject;
import model.MrTaskDetail;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import utils.Constants;

import java.util.List;

/**
 * ES相关公共业务方法
 */
public class ESCommons {

    public static IndexRequest mrRequest(String index, Object... objs) {
        JSONObject data = new JSONObject();
        data.put("mrCode",objs[0]);
        data.put("mrInPath",objs[1]);
        data.put("mrStatus",objs[2]);
        IndexRequest indexRequest = ESUtil.buildRequest(data, index, objs[0].toString());
        return indexRequest;
    }

    public static List<MrTaskDetail> findMrTaskByStatus(int status){
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.matchQuery("mrStatus", status));
        return ESUtil.INSTANCE.boolQuery(Constants.MR_INDEX_NAME, boolQuery, MrTaskDetail.class);
    }
}
