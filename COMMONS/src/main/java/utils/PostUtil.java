package utils;

import java.io.IOException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostUtil {
    public final static Logger LOGGER = LoggerFactory.getLogger(PostUtil.class);

    public static String HttpPostWithJson(String url, String json) {
        String returnValue = "query error";
        CloseableHttpClient httpClient = null;
        CloseableHttpResponse response = null;
        try {
            //第一步：创建HttpClient对象
            httpClient = HttpClients.createDefault();

            //第二步：创建httpPost对象
            HttpPost httpPost = new HttpPost(url);

            //第三步：给httpPost设置JSON格式的参数
            StringEntity requestEntity = new StringEntity(json, "utf-8");
            requestEntity.setContentEncoding("UTF-8");
            httpPost.setHeader("Content-type", "application/json");
            httpPost.setEntity(requestEntity);

            //第四步：发送HttpPost请求，获取返回值
            response = httpClient.execute(httpPost);
            returnValue = response.getStatusLine().toString();

        } catch (Exception e) {
            LOGGER.error("query error,url:{},json:{}", url, json);
        } finally {
            try {
                if(httpClient != null){
                    httpClient.close();
                }
                if(response != null){
                    response.close();
                }
            } catch (IOException e) {
                LOGGER.error("close httpclient error", e);
            }
        }
        //第五步：处理返回值
        return returnValue;
    }
}