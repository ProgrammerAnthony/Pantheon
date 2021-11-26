package com.pantheon.server;

import com.netflix.config.ConfigurationManager;
import com.pantheon.common.ShutdownHookThread;
import com.pantheon.common.lifecycle.Lifecycle;
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

        initPantheonEnvironment();
        startServerNode();
    }

    private static ServerNode startServerNode() {
        ServerNode serverNode =ServerNode.getInstance();
        serverNode.start();
        if (!serverNode.lifecycleState().equals(Lifecycle.State.STARTED)) {
            serverNode.stop();
            System.exit(-3);
        }
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(logger, (Callable<Void>) () -> {
            serverNode.stop();
            return null;
        }));
        return serverNode;
    }

    public static void shutdown(final ServerNode serverNode) {
        serverNode.stop();
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
