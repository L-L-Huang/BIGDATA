package es;

import org.elasticsearch.action.ActionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESAsyncListener implements ActionListener {
    public final static Logger LOGGER = LoggerFactory.getLogger(ESClientPoolFactory.class);

    @Override
    public void onResponse(Object o) {
        LOGGER.info("insert es success,msg={}",o.toString());
    }

    @Override
    public void onFailure(Exception e) {
        LOGGER.error("insert es fail", e);
    }
}
