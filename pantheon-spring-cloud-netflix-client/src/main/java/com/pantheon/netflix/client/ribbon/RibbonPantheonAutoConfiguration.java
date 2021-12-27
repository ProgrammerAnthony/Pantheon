package com.pantheon.netflix.client.ribbon;

import com.pantheon.netflix.client.loadbalancer.DiscoveryEnabledNIWSServerList;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.netflix.ribbon.RibbonAutoConfiguration;
import org.springframework.cloud.netflix.ribbon.RibbonClients;
import org.springframework.cloud.netflix.ribbon.SpringClientFactory;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Anthony
 * @create 2021/12/20
 * @desc Spring configuration for configuring Ribbon defaults to be Pantheon based if Pantheon client is enabled
 **/
@Configuration
@EnableConfigurationProperties
@RibbonPantheonAutoConfiguration.ConditionalOnRibbonAndPantheonEnabled
@AutoConfigureAfter({RibbonAutoConfiguration.class})
@RibbonClients(
        defaultConfiguration = {PantheonRibbonClientConfiguration.class}
)
public class RibbonPantheonAutoConfiguration {
    public RibbonPantheonAutoConfiguration() {
    }

    private static class OnRibbonAndPantheonEnabledCondition extends AllNestedConditions {
        public OnRibbonAndPantheonEnabledCondition() {
            super(ConfigurationPhase.REGISTER_BEAN);
        }

        @ConditionalOnProperty(
                value = {"pantheon.client.enabled"},
                matchIfMissing = true
        )
        static class OnPantheonClientEnabled {
            OnPantheonClientEnabled() {
            }
        }

        @ConditionalOnBean({DiscoveryClient.class})
        static class PantheonBeans {
            PantheonBeans() {
            }
        }

        @ConditionalOnClass({DiscoveryEnabledNIWSServerList.class})
        @ConditionalOnBean({SpringClientFactory.class})
        @ConditionalOnProperty(
                value = {"ribbon.pantheon.enabled"},
                matchIfMissing = true
        )
        static class Defaults {
            Defaults() {
            }
        }
    }

    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @Conditional({RibbonPantheonAutoConfiguration.OnRibbonAndPantheonEnabledCondition.class})
    @interface ConditionalOnRibbonAndPantheonEnabled {
    }
}
