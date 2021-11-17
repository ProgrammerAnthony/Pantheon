package com.pantheon.server.config;

import com.pantheon.server.Bootstrap;
import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Anthony
 * @create 2021/11/17
 * @desc A default implementation of Pantheon Server configuration as required by {@link PantheonServerConfig}.
 * <p>
 * The information required for configuring pantheon server is provided in a
 * configuration file.The configuration file is searched for in the classpath
 * with the name specified by the property <em>pantheon.server.props</em> and with
 * the suffix <em>.properties</em>. If the property is not specified,
 * <em>pantheon-server.properties</em> is assumed as the default.The properties
 * that are looked up uses the <em>namespace</em> passed on to this class.
 * </p>
 *
 * <p>
 * If the <em>pantheon.environment</em> property is specified, additionally
 * <em>pantheon-server-<pantheon.environment>.properties</em> is loaded in addition
 * to <em>pantheon-server.properties</em>.
 * </p>
 * <p>
 * todo accomplish all of the config & client config
 * todo verify how to add config in application。properties
 **/
public class DefaultPantheonServerConfig implements PantheonServerConfig {
    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);
    private static final String TEST = "test";
    private static final String PANTHEON_ENVIRONMENT = "pantheon.environment";
    private static final String ARCHAIUS_DEPLOYMENT_ENVIRONMENT = "archaius.deployment.environment";
    private static final DynamicPropertyFactory configInstance = com.netflix.config.DynamicPropertyFactory
            .getInstance();
    private static final DynamicStringProperty PANTHEON_PROPS_FILE = DynamicPropertyFactory
            .getInstance().getStringProperty("pantheon.server.props",
                    "pantheon-server");
    private String namespace = "pantheon.";
    private static final int DEFAULT_HEARTBEAT_INTERVAL = 30;

    public DefaultPantheonServerConfig() {
        init();
    }

    public DefaultPantheonServerConfig(String namespace) {
        this.namespace = namespace;
        init();
    }

    private void init() {
        String env = ConfigurationManager.getConfigInstance().getString(
                PANTHEON_ENVIRONMENT, TEST);
        ConfigurationManager.getConfigInstance().setProperty(
                ARCHAIUS_DEPLOYMENT_ENVIRONMENT, env);
        String propsFile = PANTHEON_PROPS_FILE.get();
        try {
            //load PANTHEON_PROPS_FILE after application.properties
            ConfigurationManager
                    .loadCascadedPropertiesFromResources(propsFile);
        } catch (IOException e) {
            logger.warn(
                    "Cannot find the properties specified : {}. This may be okay if there are other environment "
                            + "specific properties or the configuration is installed with a different mechanism.",
                    propsFile);
        }

    }

    @Override
    public String getDataDir() {
        String dataDir = configInstance.getStringProperty(namespace + "dataDir", "/").get();
        if (null != dataDir) {
            return dataDir.trim();
        } else {
            return null;
        }
    }

    @Override
    public String getControllerCandidateServers() {
        String servers = configInstance.getStringProperty(namespace + "controllerCandidateServers", "/").get();
        if (null != servers) {
            return servers.trim();
        } else {
            return null;
        }
    }

    @Override
    public int getHeartBeatInterval() {
        return configInstance.getIntProperty(
                namespace + "heartBeatInterval", DEFAULT_HEARTBEAT_INTERVAL).get();
    }

    @Override
    public String getNodeIp() {
        String nodeIp = configInstance.getStringProperty(namespace + "nodeIp", null).get();
        if (null != nodeIp) {
            return nodeIp.trim();
        } else {
            return null;
        }
    }

    @Override
    public int getNodeId() {
        return 0;
    }

    @Override
    public int getNodeInternTcpPort() {
        return 0;
    }

    @Override
    public int getNodeHttpPort() {
        return 0;
    }

    @Override
    public int getNodeClientTcpPort() {
        return 0;
    }

    @Override
    public boolean isControllerCandidate() {
        return false;
    }

    @Override
    public int getClusterNodeCount() {
        return 0;
    }

}


