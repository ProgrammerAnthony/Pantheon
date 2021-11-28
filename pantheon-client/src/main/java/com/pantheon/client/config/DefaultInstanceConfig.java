package com.pantheon.client.config;

import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.pantheon.client.Server;
import io.netty.bootstrap.ServerBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Anthony
 * @create 2021/11/17
 * @desc
 **/
public class DefaultInstanceConfig extends AbstractInstanceConfig implements PantheonInstanceConfig {
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);
    private static final String TEST = "test";
    private static final String PANTHEON_ENVIRONMENT = "pantheon.environment";
    private static final String ARCHAIUS_DEPLOYMENT_ENVIRONMENT = "archaius.deployment.environment";
    private static final DynamicPropertyFactory configInstance = com.netflix.config.DynamicPropertyFactory
            .getInstance();
    private static final DynamicStringProperty PANTHEON_PROPS_FILE = DynamicPropertyFactory
            .getInstance().getStringProperty("pantheon.client.props",
                    "pantheon-client");
    protected String namespace = "pantheon.";
    public static final String CONFIG_FILE_NAME = "pantheon-client";
    final String CONFIG_KEY_SERVICE_NAME = namespace + "serviceName";
    final String CONFIG_KEY_INSTANCE_HOST_NAME = namespace + "instanceHostName";
    final String CONFIG_KEY_IP_ADDRESS = namespace + "instanceIpAddress";
    final String CONFIG_KEY_CONTROLLER_CANDIDATE_SERVERS = namespace + "controllerCandidateServers";
    final String CONFIG_KEY_LEASE_RENEWAL_INTERVAL = namespace + "leaseRenewalInterval";
    final String CONFIG_KEY_LEASE_EXPIRATION_DURATION = namespace + "leaseDuration";
    final String CONFIG_KEY_INSTANCE_PORT = namespace + "instancePort";
    final String CONFIG_KEY_SHOULD_FETCH_REGISTRY = namespace + "shouldFetchRegistry";


    private List<String> serverList = new ArrayList<>();

    private static class Singleton {
        static DefaultInstanceConfig instance = new DefaultInstanceConfig();
    }

    public static DefaultInstanceConfig getInstance() {
        return Singleton.instance;
    }


    private DefaultInstanceConfig() {
        initConfig();
        validateConfig();
    }

    private DefaultInstanceConfig(String namespace) {
        initConfig();
        validateConfig();
    }


    private void initConfig() {
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

    private void validateConfig() {
//        List<Server> serverList = getServerList();
//        logger.info("-------------->" + serverList);
    }


    @Override
    public String getServiceName() {
        return configInstance.getStringProperty(CONFIG_KEY_SERVICE_NAME, null).get();
    }


    @Override
    public List<String> getServerList() {
        String servers = getControllerCandidateServers();
        String[] serverArray = servers.split(",");
        serverList = Arrays.asList(serverArray);
        return serverList;
    }

    private String getControllerCandidateServers() {
        return configInstance.getStringProperty(CONFIG_KEY_CONTROLLER_CANDIDATE_SERVERS, null).get();
    }

    @Override
    public String getInstanceHostName() {
        return configInstance.getStringProperty(CONFIG_KEY_INSTANCE_HOST_NAME, null).get();
    }

    @Override
    public String getInstanceIpAddress() {
        return configInstance.getStringProperty(CONFIG_KEY_IP_ADDRESS, null).get();
    }

    @Override
    public Integer getLeaseRenewalIntervalInSeconds() {
        return configInstance.getIntProperty(CONFIG_KEY_LEASE_RENEWAL_INTERVAL, super.getLeaseRenewalIntervalInSeconds()).get();
    }

    @Override
    public Integer getLeaseExpirationDurationInSeconds() {
        return configInstance.getIntProperty(CONFIG_KEY_LEASE_EXPIRATION_DURATION, super.getLeaseExpirationDurationInSeconds()).get();
    }


    @Override
    public Integer getInstancePort() {
        return configInstance.getIntProperty(CONFIG_KEY_INSTANCE_PORT, super.getInstancePort()).get();
    }

    @Override
    public boolean shouldFetchRegistry() {
        return configInstance.getBooleanProperty(CONFIG_KEY_SHOULD_FETCH_REGISTRY, true).get();
    }

    //todo add from eureka
    public int getRegistryFetchIntervalSeconds() {
        return 1;
    }
}
