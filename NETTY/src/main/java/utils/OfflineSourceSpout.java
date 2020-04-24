package utils;

import com.alibaba.fastjson.JSONObject;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 调试服务器的TPS/流式计算数据源
 */
public class OfflineSourceSpout {
    public static void main(String[] args) {
        String linux = "http://192.168.208.103:";
        String local = "http://192.168.208.1:";
        String nginx = "http://192.168.208.104/v1";

        //造数据规定：2w台设备，34个地区，10个渠道，4种网络制式
        String[] areas = {
                "北京","天津","河北","山西","内蒙古",
                "辽宁","吉林","黑龙江","上海","江苏",
                "浙江","安徽","福建","江西","山东",
                "河南","湖北","湖南","广东","广西",
                "海南","重庆","四川","贵州","云南",
                "西藏","陕西","甘肃","青海","宁夏",
                "新疆","台湾","香港","澳门",
        };
        String[] channels = {
                "IOS市场","Android市场","华为市场","荣耀市场","小米市场",
                "OPPO市场","VIVO市场","魅族市场","豌豆荚","应用商店"
        };
        String[] standards = {"2G","3G","4G","5G"};

        int threads = 1;
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            //随机给定服务器端口
            int random = (int) (Math.random() * 100000) % 199;
            String url = new StringBuilder(local).append(9900 + random).toString();

            executorService.scheduleWithFixedDelay(() -> {
                //随机给定数据
                int uuidRandom = (int) (Math.random() * 100000000) % 100;
                int areaRandom = (int) (Math.random() * 10000) % 34;
                int channelRandom = (int) (Math.random() * 100) % 10;
                int standardRandom = (int) (Math.random() * 100) % 4;

                String uuid = new StringBuilder("hl-test-mr-").append(uuidRandom).toString();
                JSONObject data = buildJSONObject(uuid, areas[areaRandom], channels[channelRandom], standards[standardRandom]);
                System.out.println("data request send, data:" + data.toString());
                System.out.println("response:" + PostUtil.HttpPostWithJson(url, data.toJSONString()));
            }, 1, 10, TimeUnit.SECONDS);
        }
    }

    public static JSONObject buildJSONObject(String uuid,String area,String channel,String standard){
        JSONObject data = new JSONObject();
        data.put("uuid",uuid);
        data.put("area",area);
        data.put("channel",channel);
        data.put("standard",standard);
        data.put("time",System.currentTimeMillis());
        return data;
    }
}
