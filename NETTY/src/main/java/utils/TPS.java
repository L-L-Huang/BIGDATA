package utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 调试服务器的TPS
 */
public class TPS {
    public static void main(String[] args) {
//        String url = "http://192.168.208.103:9999";
//        String url = "http://192.168.208.104/v1";
        //0-99的随机数
        String json = "{\"query\":{\"bool\":{\"must\":[{\"match_phrase\":{\"imtype\":\"LTCUS\"}},{\"match_phrase\":{\"rtdatetime\":1521164922000}}]}}}";
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1000);
        for (int i = 0; i < 1000; i++) {
            int random = (int) (Math.random() * 100000) % 199;
            int port = 9900 + random;
            String url = "http://192.168.208.103:".concat(String.valueOf(port));
            executorService.scheduleWithFixedDelay(() -> {
                    System.out.println(PostUtil.HttpPostWithJson(url, json));
            }, 1, 1, TimeUnit.SECONDS);
        }
    }
}
