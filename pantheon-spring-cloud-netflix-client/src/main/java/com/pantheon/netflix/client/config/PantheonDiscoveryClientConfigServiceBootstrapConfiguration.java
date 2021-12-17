package com.pantheon.netflix.client.config;

import com.pantheon.netflix.client.PantheonClientAutoConfiguration;
import com.pantheon.netflix.client.PantheonDiscoveryClientConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.config.client.ConfigServicePropertySourceLocator;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * @author Anthony
 * @create 2021/12/17
 * @desc
 **/
@ConditionalOnClass({ConfigServicePropertySourceLocator.class})
@ConditionalOnProperty(
        value = {"spring.cloud.config.discovery.enabled"},
        matchIfMissing = false
)
@Configuration
@Import({PantheonDiscoveryClientConfiguration.class, PantheonClientAutoConfiguration.class})
public class PantheonDiscoveryClientConfigServiceBootstrapConfiguration {
    public PantheonDiscoveryClientConfigServiceBootstrapConfiguration() {
    }
}
