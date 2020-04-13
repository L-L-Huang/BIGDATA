package server;

import com.alibaba.fastjson.JSONObject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpServerChannelHandler extends ChannelInboundHandlerAdapter {

	public final static Logger LOGGER = LoggerFactory.getLogger(HttpServerChannelHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        String responseMsg = "success";
        try {
            FullHttpRequest req = (FullHttpRequest) msg;
            int reqDataLen = req.content().readableBytes();
            if (reqDataLen == 0) {
                LOGGER.warn("request data length = {}", reqDataLen);
                ctx.writeAndFlush(buildResponse("fail")).addListener(future -> ctx.close());
                responseMsg = "fail";
                return;
            }
            byte[] content = new byte[reqDataLen];
            req.content().readBytes(content);
            JSONObject data = JSONObject.parseObject(new String(content, StandardCharsets.UTF_8));
            System.out.println(data.toString());
            responseMsg = "success";
        } catch (Exception e) {
            LOGGER.error("error netty",e);
        } finally {
            ctx.writeAndFlush(buildResponse(responseMsg)).addListener(future -> ctx.close());
            ReferenceCountUtil.release(msg);
        }

    }

    private FullHttpResponse buildResponse(String msg){
        byte[] responseMsg = msg.getBytes();
        ByteBuf content = Unpooled.wrappedBuffer(responseMsg);
        FullHttpResponse res = new DefaultFullHttpResponse(HTTP_1_1, OK, content);
        res.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream;charset=UTF-8");
        res.headers().set(HttpHeaderNames.CONTENT_LENGTH, responseMsg.length);
        return res;
    }

	@Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }

}
