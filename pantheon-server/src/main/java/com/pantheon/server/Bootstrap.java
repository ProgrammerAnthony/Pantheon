package com.pantheon.server;

import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicPropertyFactory;
import com.pantheon.server.config.DefaultPantheonServerConfig;
import com.pantheon.server.config.PantheonServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Anthony
 * @create 2021/11/17
 * @desc
 **/
public class Bootstrap {
    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);
    private static final String PANTHEON_ENVIRONMENT = "pantheon.environment";
    private static final String ARCHAIUS_DEPLOYMENT_ENVIRONMENT = "archaius.deployment.environment";
    private static final String TEST = "test";


    public static void main(String[] args) {
        logger.info("Bootstrap initializing");
        PantheonServerConfig serverConfig = new DefaultPantheonServerConfig();
        initPantheonEnvironment();
        startServerNode(serverConfig);
    }

    private static void startServerNode(PantheonServerConfig serverConfig) {
        logger.info("start server node on: "+serverConfig.getNodeIp()+":"+serverConfig.getNodeHttpPort());
        // todo initialize server node
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
