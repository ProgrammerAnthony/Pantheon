package com.pantheon.server;

import com.pantheon.common.RequestCode;
import com.pantheon.common.ServiceState;
import com.pantheon.common.ThreadFactoryImpl;
import com.pantheon.common.exception.InitException;
import com.pantheon.common.protocol.ResponseCode;
import com.pantheon.common.protocol.header.GetServerNodeIdRequestHeader;
import com.pantheon.common.protocol.header.GetServerNodeIdResponseHeader;
import com.pantheon.remoting.RemotingServer;
import com.pantheon.remoting.common.RemotingHelper;
import com.pantheon.remoting.exception.RemotingCommandException;
import com.pantheon.remoting.netty.AsyncNettyRequestProcessor;
import com.pantheon.remoting.netty.NettyRemotingServer;
import com.pantheon.remoting.netty.NettyRequestProcessor;
import com.pantheon.remoting.netty.NettyServerConfig;
import com.pantheon.remoting.protocol.RemotingCommand;
import com.pantheon.server.config.PantheonServerConfig;
import com.pantheon.server.processor.ServerNodeProcessor;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author Anthony
 * @create 2021/11/18
 * @desc 1 todo build BIO for server inside communication
 * 2 todo rethink and build the mechanism of client and server RocketMq
 * 3 todo build heartbeat mechanism of client and server
 * 4 todo design the message protocol
 * 5 todo design slots mechanism and treat it as topic in RocketMq
 **/
public class ServerController {
    private NettyServerConfig nettyServerConfig;
    private PantheonServerConfig serverConfig;
    private RemotingServer remotingServer;
    private ExecutorService remotingExecutor;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "ServerControllerScheduledThread"));
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);
    private ServiceState serviceState = ServiceState.CREATE_JUST;

    public ServerController(PantheonServerConfig serverConfig, NettyServerConfig nettyServerConfig) {
        this.nettyServerConfig = nettyServerConfig;
        this.serverConfig = serverConfig;
    }

    public boolean initialize() {
        synchronized (ServerController.class) {
            switch (serviceState) {
                case CREATE_JUST:
                    serviceState = ServiceState.START_FAILED;
                    nettyServerConfig.setListenPort(serverConfig.getNodeClientTcpPort());
                    remotingServer = new NettyRemotingServer(nettyServerConfig);
                    this.remotingExecutor =
                            Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));
                    remotingServer.registerDefaultProcessor(new ServerNodeProcessor(this), remotingExecutor);
                    remotingServer.start();
                    this.startScheduledTask();
                    serviceState = ServiceState.RUNNING;
                    break;
                case START_FAILED:
                    throw new InitException("ServerBootstrap failed to initialize ");
                default:
                    break;
            }
        }
        return true;
    }

    private void startScheduledTask() {

        //heartbeat check with connected instances，remove expired ones
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    ServerController.this.heartBeatCheck();
                } catch (Exception e) {
                    logger.error("ScheduledTask heartBeatCheck exception", e);
                }
            }
        }, 5000, serverConfig.getHeartBeatCheckInterval(), TimeUnit.MILLISECONDS);
    }


    private void heartBeatCheck() {

    }


    public void shutdown() {
        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    break;
                case RUNNING:
                    this.remotingServer.shutdown();
                    this.remotingExecutor.shutdown();
                    logger.info("the server controller shutdown OK");
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                default:
                    break;
            }
        }
    }

    public PantheonServerConfig getServerConfig() {
        return serverConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }
}
