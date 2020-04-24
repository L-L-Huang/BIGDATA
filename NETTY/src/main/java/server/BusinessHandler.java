package server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import kafka.ProducerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Constants;

/**
 * 业务处理器
 */
public class BusinessHandler implements Runnable{
    public final static Logger LOGGER = LoggerFactory.getLogger(BusinessHandler.class);
    private ChannelHandlerContext ctx;
    private Object msg;
    public BusinessHandler(ChannelHandlerContext ctx,Object msg){
        this.ctx = ctx;
        this.msg = msg;
    }

    @Override
    public void run() {
        try {
            FullHttpRequest req = (FullHttpRequest) msg;
            int reqDataLen = req.content().readableBytes();
            if (reqDataLen == 0) {
                LOGGER.warn("request data length = {}", reqDataLen);
                return;
            }
            byte[] content = new byte[reqDataLen];
            req.content().readBytes(content);
            ProducerClient.instance.sendMsg(Constants.MR_TOPIC, content);
        } catch (Exception e) {
            LOGGER.error("error netty",e);
        } finally {
            ctx.writeAndFlush(buildResponse(Constants.HTTP_SERVER_RESPONSE)).addListener(future -> ctx.close());
            ReferenceCountUtil.release(msg);
        }
    }

    private FullHttpResponse buildResponse(String msg){
        byte[] responseMsg = msg.getBytes();
        ByteBuf content = Unpooled.wrappedBuffer(responseMsg);
        FullHttpResponse res = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
        res.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream;charset=UTF-8");
        res.headers().set(HttpHeaderNames.CONTENT_LENGTH, responseMsg.length);
        return res;
    }
}
