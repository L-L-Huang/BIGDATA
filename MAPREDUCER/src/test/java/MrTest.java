import es.ESCommons;
import model.MrTaskDetail;
import mr.enums.MrTaskStatus;
import mr.utils.ParquetUtil;
import org.junit.Test;
import java.util.List;

public class MrTest {

    @Test
    public void query(){
        String mrCode = ParquetUtil.getMrTaskCode(System.currentTimeMillis());
        List<MrTaskDetail> list = ESCommons.findMrTaskByStatus(MrTaskStatus.ACTIVE.getCode(), mrCode);
        System.out.println(list.toString());
    }

}
