package com.hazelcast;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

public class NettyBounceBenchmark {

    public static int port = 5000;
    public static EventLoopGroup bossGroup;
    public static int concurrency = 1;
    public static int iterations = 20;

    public static void main(String[] args) throws InterruptedException {

        EventLoopGroup clientWorkerGroup = new NioEventLoopGroup();

          EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        ChannelFuture sf = serverBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new PingPongChannelInitializer(new PingPongServerHandler()))
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .bind(port).sync();
        sf.await();
        sf.channel();

        Bootstrap clientBootstrap = new Bootstrap();
        ChannelFuture cf = clientBootstrap.group(clientWorkerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new PingPongChannelInitializer(new PingPongClientHandler()))
                .connect("127.0.0.1", port).sync();
        cf.await();


    }

    @ChannelHandler.Sharable
    static class PingPongServerHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead (ChannelHandlerContext ctx, Object msg) {
            ctx.write (msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }

        @Override
        public void exceptionCaught (ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    static class PingPongClientHandler extends ChannelInboundHandlerAdapter {

        final int M = 1000, N = 10000;

        long start = System.currentTimeMillis();
        int i = M, j = N;

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            ctx.writeAndFlush("Hello World.\n");
            j--;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ctx.write("Hello World.\n");
            j--;
            if (j == 0) {
                long end = System.currentTimeMillis();
                double ms = (double) (end - start) / (double) (N);
                double qps = 1.0 / ms * 1000.0;
                System.out.println(String.format("%d trials: %10.3f ms, %10.0f rps", N, ms, qps));
                j = N;
                i--;
                if (i == 0)
                    ctx.close();
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    static class PingPongChannelInitializer extends ChannelInitializer<SocketChannel> {

        private ChannelHandler handler;

        public PingPongChannelInitializer(ChannelHandler handler) {
            this.handler = handler;
        }

        @Override
        public void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline().addLast(
                    new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 2),
                    new LengthFieldPrepender(2),
                    new StringDecoder(CharsetUtil.UTF_8),
                    new StringEncoder(CharsetUtil.UTF_8),
                    handler);
        }
    }
}
