package server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.*;

public class HttpServer {

    private final static Logger LOGGER = LoggerFactory.getLogger(HttpServer.class);
    protected EventLoopGroup bossGroup;
    protected EventLoopGroup workerGroup;

    public static void main(String[] args) {
        if (args.length < 4) {
            LOGGER.error("Missing startup parameters");
            System.exit(0);
        }
        new HttpServer().start(args);
    }

    public void start(String[] args) {
        createNioServer(args);
    }

    /**
     * 释放线程池和句柄
     */
    public void stop() {
        LOGGER.info("shutdown {}...", this.getClass().getSimpleName());
        if (bossGroup != null) {
            bossGroup.shutdownGracefully().syncUninterruptibly();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully().syncUninterruptibly();
        }
        LOGGER.info("{} shutdown success.", this.getClass().getSimpleName());
        System.exit(0);
    }

    /**
     * 创建NIO模型
     */
    private void createNioServer(String[] args) {
        int bossThreads = Integer.valueOf(args[0]);
        int workThreads = Integer.valueOf(args[1]);
        int socketQueueSize = Integer.valueOf(args[2]);
        int workIoRatio = Integer.valueOf(args[3]);

        //接收客户端的连接
        EventLoopGroup bossGroup = getBossGroup();
        //处理IO任务
        EventLoopGroup workerGroup = getWorkerGroup();

        if (bossGroup == null) {
            //池化LoopGroup操作使得并发量增加，且响应模式变更为多线程串行响应
            NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup(bossThreads, getBossThreadFactory(), SelectorProvider.provider());
            //boss组全部用于处理IO
            nioEventLoopGroup.setIoRatio(100);
            bossGroup = nioEventLoopGroup;
        }

        if (workerGroup == null) {
            NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup(workThreads, getWorkThreadFactory(), SelectorProvider.provider());
            //设置所有 EventLoop 的 IO 任务占用执行时间的比例，IO占70%业务流程30%，默认50，1：1
            nioEventLoopGroup.setIoRatio(workIoRatio);
            workerGroup = nioEventLoopGroup;
        }

        createServer(bossGroup, workerGroup, getChannelFactory(), socketQueueSize);

    }

    /**
     * 创建epoll模型
     */
    private void createEpollServer(String[] args) {
        int bossThreads = Integer.valueOf(args[0]);
        int workThreads = Integer.valueOf(args[1]);
        int socketQueueSize = Integer.valueOf(args[2]);
        int workIoRatio = Integer.valueOf(args[3]);

        EventLoopGroup bossGroup = getBossGroup();
        EventLoopGroup workerGroup = getWorkerGroup();

        if (bossGroup == null) {
            EpollEventLoopGroup epollEventLoopGroup = new EpollEventLoopGroup(bossThreads, getBossThreadFactory());
            epollEventLoopGroup.setIoRatio(100);
            bossGroup = epollEventLoopGroup;
        }

        if (workerGroup == null) {
            EpollEventLoopGroup epollEventLoopGroup = new EpollEventLoopGroup(workThreads, getWorkThreadFactory());
            epollEventLoopGroup.setIoRatio(workIoRatio);
            workerGroup = epollEventLoopGroup;
        }

        createServer(bossGroup, workerGroup, new ReflectiveChannelFactory<ServerChannel>(EpollServerSocketChannel.class), socketQueueSize);
    }

    /**
     * @param boss
     * @param work
     * @param channelFactory
     */
    private void createServer(EventLoopGroup boss, EventLoopGroup work, ChannelFactory<? extends ServerChannel> channelFactory, int socketQueueSize) {
        this.bossGroup = boss;
        this.workerGroup = work;
        int port = 9900;
        int beginPort = 9900;
        ChannelFuture f = null;
        int nPort = Integer.valueOf(System.getProperty("nPort"));
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup);
            b.channelFactory(channelFactory);
            b.childHandler(new ChannelInitializer<Channel>() {
                @Override
                public void initChannel(Channel ch) throws Exception {
                    //每连上一个链接调用一次
                    initPipeline(ch.pipeline());
                }
            });
            initOptions(b, socketQueueSize);
            //绑定100个端口
            for (int i = 0; i < nPort; i++) {
                port = beginPort + i;
                f = b.bind("0.0.0.0", port).sync();
                LOGGER.info("HttpServer start success, port {}", port);
            }
            if(f == null){
                LOGGER.error("ChannelFuture is null");
                System.exit(0);
            }
            f.channel().closeFuture().sync();
        } catch (Exception e) {
            LOGGER.error("server start exception,port:{}", port, e);
        } finally {
            stop();
        }
    }

    protected void initOptions(ServerBootstrap b, int socketQueueSize) {
        //option设置boss线程组，childOption设置work线程组
        b.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        //Socket队列最大长度，在接受HTTP请求时尤其注意调整参数
        b.option(ChannelOption.SO_BACKLOG, socketQueueSize);
        //使用对象池，重用缓冲区
        b.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        //TCP链接超过一定时间发ack包，禁用
        b.childOption(ChannelOption.SO_KEEPALIVE, false);
        //禁用nagle算法,不等待立即响应
        b.childOption(ChannelOption.TCP_NODELAY, true);
    }

    /**
     * 规定http请求参数的限制
     *
     * @param pipeline
     */
    protected void initPipeline(ChannelPipeline pipeline) {
        pipeline.addLast("handler", new HttpServerChannelHandler());
        pipeline.addBefore("handler", "encaps", new HttpObjectAggregator(10240000));
        pipeline.addBefore("encaps", "codec", new HttpServerCodec());
    }

    /**
     * @return
     */
    protected ThreadFactory getBossThreadFactory() {
        return new DefaultThreadFactory("boss-thread");
    }

    /**
     * Netty处理I/O操作的Reactor线程池职责如下
     * （1）异步读取通信对端的数据报，发送读事件到ChannelPipeline；
     * （2）异步发送消息到通信对端，调用ChannelPipeline的消息发送接口；
     *
     * @return
     */
    public EventLoopGroup getWorkerGroup() {
        return bossGroup;
    }

    /**
     * Netty用于接收客户端请求的线程池职责如下
     * （1）接收客户端TCP连接，初始化Channel参数；
     * （2）将链路状态变更事件通知给ChannelPipeline。
     *
     * @return
     */
    public EventLoopGroup getBossGroup() {
        return workerGroup;
    }

    /**
     * @return
     */
    public ThreadFactory getWorkThreadFactory() {
        return new DefaultThreadFactory("work-thread");
    }

    public ChannelFactory<? extends ServerChannel> getChannelFactory() {
        return new ReflectiveChannelFactory<ServerChannel>(NioServerSocketChannel.class);
    }

}
