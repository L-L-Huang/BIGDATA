package elasticsearch;

import com.alibaba.fastjson.JSONObject;
import es.ESAsyncListener;
import es.ESPoolUtil;
import es.ESUtil;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

public class CompanyTest {

    /**
     * 创建index并录入单条数据
     * @throws Exception
     */
    @Test
    public void createIndex(){
        IndexRequest request = new IndexRequest("company").id("1");
        JSONObject employee = new JSONObject();
        employee.put("name","lampard");
        employee.put("join_date","2013-01-30");
        employee.put("salary",10000);
        request.source(employee.toJSONString(), XContentType.JSON);
//        IndexResponse indexResponse = ESPoolUtil.getClient().index(request,RequestOptions.DEFAULT);
        RestHighLevelClient client = null;
        try {
            client = ESPoolUtil.getClient();
            client.indexAsync(request,RequestOptions.DEFAULT, new ESAsyncListener());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ESPoolUtil.returnClient(client);
        }
    }

    /**
     * 单次插入
     */
    @Test
    public void insertEmployee() throws InterruptedException {
        JSONObject employee = new JSONObject();
        employee.put("name","drogba");
        employee.put("join_date","2013-01-30");
        employee.put("salary",10000);
        ESUtil.insertAsync("company",employee,null);
        Thread.sleep(5000);
    }

    /**
     * 批量数据入库
     */
    @Test
    public void batchAddTest() {
        List<IndexRequest> requests = generateRequests();
        ESUtil.insertBatchAsync(requests);
    }

    public List<IndexRequest> generateRequests(){
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(generateEmployeesRequest("李四","2018-01-02","20000"));
        requests.add(generateEmployeesRequest("王五","2011-01-02","20000"));
        requests.add(generateEmployeesRequest("赵六","2020-01-02","30000"));
        return requests;
    }

    public IndexRequest generateEmployeesRequest(Object name, Object join_date, Object salary) {
        IndexRequest indexRequest = new IndexRequest("company");
        JSONObject employee = new JSONObject();
        employee.put("name",name);
        employee.put("join_date",join_date);
        employee.put("salary",salary);
        indexRequest.source(employee.toJSONString(), XContentType.JSON);
        return indexRequest;
    }
}
