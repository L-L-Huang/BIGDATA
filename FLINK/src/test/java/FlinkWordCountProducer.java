import string.ProducerClientString;
import org.junit.Test;
import utils.Constants;

public class FlinkWordCountProducer {

    @Test
    public void sendQueryData() throws InterruptedException {
        while(true){
            String text = "hello 曹操,hello 曹操,hello 刘备 孙权,hello 刘备 孙权";
            ProducerClientString.instance.sendMsg(Constants.FLINK_TOPIC, text);
//            System.out.println("ddbook query data send kafka success");
            Thread.sleep(1000);
        }
    }

    @Test
    public void splitStr() {
        String word = "hello 切尔西,hello 曼联,hello 曼联 阿森纳,hello 切尔西 利物浦";
        String[] arr = word.split("[,\\s]");
        System.out.println(arr.toString());
    }
}
