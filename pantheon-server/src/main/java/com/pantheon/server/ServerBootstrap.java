package com.pantheon.server;

import com.netflix.config.ConfigurationManager;
import com.pantheon.common.ShutdownHookThread;
import com.pantheon.remoting.netty.NettyServerConfig;
import com.pantheon.server.config.DefaultPantheonServerConfig;
import com.pantheon.server.config.PantheonServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

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
        logger.info("ServerBootstrap initializing......");
        PantheonServerConfig serverConfig = DefaultPantheonServerConfig.getInstance();
        initPantheonEnvironment();
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        ServerController serverController = new ServerController(serverConfig, nettyServerConfig);
        start(serverController);
    }

    private static ServerController start(ServerController serverController) {
        boolean initResult = serverController.initialize();
        if (!initResult) {
            serverController.shutdown();
            System.exit(-3);
        }
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(logger, (Callable<Void>) () -> {
            serverController.shutdown();
            return null;
        }));
        //start netty
        serverController.start();

        return serverController;
    }

    public static void shutdown(final ServerController controller) {
        controller.shutdown();
    }


    /**
     * Users can override to initialize the environment themselves.
     */
    protected static void initPantheonEnvironment() {
        String environment = ConfigurationManager.getConfigInstance().getString(PANTHEON_ENVIRONMENT);
        if (environment == null) {
            //default test environment
            ConfigurationManager.getConfigInstance().setProperty(ARCHAIUS_DEPLOYMENT_ENVIRONMENT, TEST);
            logger.info(PANTHEON_ENVIRONMENT + " value is not set, defaulting to test");
        }
        logger.info("Setting the Pantheon configuration withe environment " + environment);
    }
}
