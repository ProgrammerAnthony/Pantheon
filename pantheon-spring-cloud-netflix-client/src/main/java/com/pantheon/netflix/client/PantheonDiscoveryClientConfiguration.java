package com.pantheon.netflix.client;

import com.pantheon.client.DiscoveryClientNode;
import com.pantheon.client.config.PantheonInstanceConfig;
import com.pantheon.netflix.client.serviceregistry.PantheonAutoServiceRegistration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.HealthAggregator;
import org.springframework.boot.actuate.health.OrderedHealthAggregator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.context.scope.refresh.RefreshScopeRefreshedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

/**
 * @author Anthony
 * @create 2021/12/17
 * @desc
 **/
@Configuration
@EnableConfigurationProperties
@ConditionalOnClass({PantheonInstanceConfig.class})
@ConditionalOnProperty(
        value = {"pantheon.client.enabled"},
        matchIfMissing = true
)
public class PantheonDiscoveryClientConfiguration {
    public PantheonDiscoveryClientConfiguration() {
    }

    @Bean
    public PantheonDiscoveryClientConfiguration.Marker eurekaDiscoverClientMarker() {
        return new PantheonDiscoveryClientConfiguration.Marker();
    }

    @Configuration
    @ConditionalOnProperty(
            value = {"pantheon.client.healthcheck.enabled"},
            matchIfMissing = false
    )
    protected static class EurekaHealthCheckHandlerConfiguration {
        @Autowired(
                required = false
        )
        private HealthAggregator healthAggregator = new OrderedHealthAggregator();

        protected EurekaHealthCheckHandlerConfiguration() {
        }

        @Bean
        @ConditionalOnMissingBean({HealthCheckHandler.class})
        public PantheonHealthCheckHandler eurekaHealthCheckHandler() {
            return new PantheonHealthCheckHandler(this.healthAggregator);
        }
    }

    @Configuration
    @ConditionalOnClass({RefreshScopeRefreshedEvent.class})
    protected static class EurekaClientConfigurationRefresher {
        @Autowired(
                required = false
        )
        private DiscoveryClientNode pantheonClient;
        @Autowired(
                required = false
        )
        private PantheonAutoServiceRegistration autoRegistration;

        protected EurekaClientConfigurationRefresher() {
        }

        @EventListener({RefreshScopeRefreshedEvent.class})
        public void onApplicationEvent(RefreshScopeRefreshedEvent event) {
            if (this.pantheonClient != null) {
                this.pantheonClient.getApplications();
            }

            if (this.autoRegistration != null) {
                this.autoRegistration.stop();
                this.autoRegistration.start();
            }

        }
    }

    class Marker {
        Marker() {
        }
    }
}
