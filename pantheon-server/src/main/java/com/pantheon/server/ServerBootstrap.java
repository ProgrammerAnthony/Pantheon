package com.pantheon.server;

import com.netflix.config.ConfigurationManager;
import com.pantheon.remoting.RemotingServer;
import com.pantheon.remoting.netty.AsyncNettyRequestProcessor;
import com.pantheon.remoting.netty.NettyRemotingServer;
import com.pantheon.remoting.netty.NettyRequestProcessor;
import com.pantheon.remoting.netty.NettyServerConfig;
import com.pantheon.remoting.protocol.RemotingCommand;
import com.pantheon.server.config.DefaultPantheonServerConfig;
import com.pantheon.server.config.PantheonServerConfig;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.util.concurrent.Executors;

/**
 * @author Anthony
 * @create 2021/11/17
 * @desc
 **/
public class ServerBootstrap {
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);
    private static final String PANTHEON_ENVIRONMENT = "pantheon.environment";
    private static final String ARCHAIUS_DEPLOYMENT_ENVIRONMENT = "archaius.deployment.environment";
    private static final String TEST = "test";

    public static void main(String[] args) {
        logger.info("Bootstrap initializing");
        PantheonServerConfig serverConfig = new DefaultPantheonServerConfig();
        initPantheonEnvironment();
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        start(new ServerController(serverConfig,nettyServerConfig));
    }

    private static void start(ServerController serverController) {
        boolean initResult = serverController.initialize();
        if(!initResult){
            serverController.shutdown();
        }

    }



    /**
     * Users can override to initialize the environment themselves.
     */
    protected static void initPantheonEnvironment() {
        logger.info("Setting the Pantheon configuration..");
        String environment = ConfigurationManager.getConfigInstance().getString(PANTHEON_ENVIRONMENT);
        if (environment == null) {
            //default test environment
            ConfigurationManager.getConfigInstance().setProperty(ARCHAIUS_DEPLOYMENT_ENVIRONMENT, TEST);
            logger.info("Pantheon environment value pantheon.environment is not set, defaulting to test");
        }
//        logger.info("---------->complete load config, log data dir: " + serverConfig.getDataDir());
    }
}
