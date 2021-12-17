package com.pantheon.netflix.client.config;

import com.pantheon.client.DiscoveryClientNode;
import com.pantheon.netflix.client.PantheonDiscoveryClientConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;

import javax.annotation.PostConstruct;

/**
 * @author Anthony
 * @create 2021/12/17
 * @desc
 **/
@ConditionalOnBean({PantheonDiscoveryClientConfiguration.class})
@ConditionalOnProperty(
        value = {"spring.cloud.config.discovery.enabled"},
        matchIfMissing = false
)
public class PantheonDiscoveryClientConfigServiceAutoConfiguration {
    @Autowired
    private ConfigurableApplicationContext context;

    public PantheonDiscoveryClientConfigServiceAutoConfiguration() {
    }

    @PostConstruct
    public void init() {
        if (this.context.getParent() != null && this.context.getBeanNamesForType(DiscoveryClientNode.class).length > 0 && this.context.getParent().getBeanNamesForType(DiscoveryClientNode.class).length > 0) {
            ((DiscoveryClientNode)this.context.getParent().getBean(DiscoveryClientNode.class)).shutdown();
        }

    }
}
