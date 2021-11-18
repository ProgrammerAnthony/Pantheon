package com.pantheon.server;

import com.pantheon.remoting.RemotingServer;
import com.pantheon.remoting.netty.AsyncNettyRequestProcessor;
import com.pantheon.remoting.netty.NettyRemotingServer;
import com.pantheon.remoting.netty.NettyRequestProcessor;
import com.pantheon.remoting.netty.NettyServerConfig;
import com.pantheon.remoting.protocol.RemotingCommand;
import com.pantheon.server.config.PantheonServerConfig;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

/**
 * @author Anthony
 * @create 2021/11/18
 * @desc
 **/
public class ServerController {
    private NettyServerConfig nettyServerConfig;
    private PantheonServerConfig serverConfig;
    private RemotingServer remotingServer;
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);

    public ServerController(PantheonServerConfig serverConfig, NettyServerConfig nettyServerConfig) {
        this.nettyServerConfig = nettyServerConfig;
        this.serverConfig = serverConfig;
    }

    public boolean initialize() {
        nettyServerConfig.setListenPort(serverConfig.getNodeClientTcpPort());
        remotingServer = new NettyRemotingServer(nettyServerConfig);
        remotingServer.registerProcessor(0, new AsyncNettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                request.setRemark("Hi " + ctx.channel().remoteAddress());
                return request;
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        }, Executors.newCachedThreadPool());

        remotingServer.start();


        remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(), Executors.newCachedThreadPool());
        return true;
    }

    public void shutdown() {
        this.remotingServer.shutdown();
    }

    public class DefaultRequestProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {

        @Override
        public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
            logger.info("start server node on: " + serverConfig.getNodeIp() + ":" + serverConfig.getNodeClientTcpPort());
            logger.info("request:" + request);
            return request;
        }

        @Override
        public boolean rejectRequest() {
            return false;
        }
    }
}
