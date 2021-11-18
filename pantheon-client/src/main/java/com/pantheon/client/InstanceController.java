package com.pantheon.client;

import com.pantheon.client.config.DefaultInstanceConfig;
import com.pantheon.common.ThreadFactoryImpl;
import com.pantheon.remoting.netty.NettyClientConfig;
import com.pantheon.remoting.netty.NettyRemotingClient;
import com.pantheon.remoting.netty.NettyRemotingServer;
import io.netty.bootstrap.ServerBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Anthony
 * @create 2021/11/19
 * @desc
 **/
public class InstanceController {
    private NettyClientConfig nettyClientConfig;
    private DefaultInstanceConfig instanceConfig;
    private ExecutorService remotingExecutor;
    private NettyRemotingClient nettyRemotingClient;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "InstanceControllerScheduledThread"));
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);

    public InstanceController(NettyClientConfig nettyClientConfig, DefaultInstanceConfig instanceConfig) {
        this.nettyClientConfig = nettyClientConfig;
        this.instanceConfig = instanceConfig;
    }

    public boolean initialize() {
        nettyRemotingClient = new NettyRemotingClient(nettyClientConfig);
        this.remotingExecutor =
                Executors.newFixedThreadPool(nettyClientConfig.getClientWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));
        return true;
    }

    public void start() {
        this.nettyRemotingClient.start();
    }

    public void shutdown() {
        this.nettyRemotingClient.shutdown();
        this.remotingExecutor.shutdown();

    }
}
