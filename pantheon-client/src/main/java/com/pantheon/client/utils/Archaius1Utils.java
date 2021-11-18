package com.pantheon.client.utils;

import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This is an INTERNAL class not for public use.
 *
 */
public final class Archaius1Utils {

    private static final Logger logger = LoggerFactory.getLogger(Archaius1Utils.class);
    private static final String TEST = "test";
    private static final String ARCHAIUS_DEPLOYMENT_ENVIRONMENT = "archaius.deployment.environment";
    private static final String PANTHEON_ENVIRONMENT = "pantheon.environment";

    public static DynamicPropertyFactory initConfig(String configName) {

        DynamicPropertyFactory configInstance = DynamicPropertyFactory.getInstance();
        DynamicStringProperty PANTHEON_PROPS_FILE = configInstance.getStringProperty("pantheon.client.props", configName);

        String env = ConfigurationManager.getConfigInstance().getString(PANTHEON_ENVIRONMENT, TEST);
        ConfigurationManager.getConfigInstance().setProperty(ARCHAIUS_DEPLOYMENT_ENVIRONMENT, env);

        String pantheonPropsFile = PANTHEON_PROPS_FILE.get();
        try {
            ConfigurationManager.loadCascadedPropertiesFromResources(pantheonPropsFile);
        } catch (IOException e) {
            logger.warn(
                    "Cannot find the properties specified : {}. This may be okay if there are other environment "
                            + "specific properties or the configuration is installed with a different mechanism.",
                    pantheonPropsFile);

        }

        return configInstance;
    }
}
