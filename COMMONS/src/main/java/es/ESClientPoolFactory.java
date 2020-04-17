package es;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * EliasticSearch连接池工厂对象
 */
public class ESClientPoolFactory implements PooledObjectFactory<RestHighLevelClient> {
    public final static Logger LOGGER = LoggerFactory.getLogger(ESClientPoolFactory.class);
    @Override
    public void activateObject(PooledObject<RestHighLevelClient> arg0) {
        LOGGER.info("activateObject");
    }
    
    /**
     * 销毁对象
     */
    @Override
    public void destroyObject(PooledObject<RestHighLevelClient> pooledObject) throws Exception {
        RestHighLevelClient highLevelClient = pooledObject.getObject();
        if(highLevelClient != null){
            highLevelClient.close();
        }
    }
    
    /**
     * 生产对象
     */
    @Override
    public PooledObject<RestHighLevelClient> makeObject() throws IOException {
//      Settings settings = Settings.builder().put("cluster.name","elasticsearch").build();
        RestHighLevelClient client = null;
        try {
            /*client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));*/
            client = new RestHighLevelClient(RestClient.builder(
                    new HttpHost("192.168.208.103", 9200, "http"),
                    new HttpHost("192.168.208.104", 9200, "http"),
                    new HttpHost("192.168.208.105", 9200, "http")));
 
        } catch (Exception e) {
            LOGGER.error("init es cluster error",e);
            if(client != null){
                client.close();
            }
        }
        return new DefaultPooledObject<>(client);
    }
 
    @Override
    public void passivateObject(PooledObject<RestHighLevelClient> arg0) {
        LOGGER.info("passivateObject");
    }
 
    @Override
    public boolean validateObject(PooledObject<RestHighLevelClient> arg0) {
        return true;
    }   
}