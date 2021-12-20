package com.pantheon.netflix.client.serviceregistry;

import com.pantheon.client.DiscoveryClientNode;
import com.pantheon.client.config.PantheonInstanceConfig;
import com.pantheon.netflix.client.HealthCheckHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.util.Assert;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Anthony
 * @create 2021/12/17
 * @desc
 **/
public class PantheonRegistration implements Registration {
    private static final Log log = LogFactory.getLog(PantheonRegistration.class);
    private final DiscoveryClientNode pantheonClient;
    private final AtomicReference<DiscoveryClientNode> cloudPantheonClient;
    private final PantheonInstanceConfig instanceConfig;
    private HealthCheckHandler healthCheckHandler;

    private PantheonRegistration(PantheonInstanceConfig instanceConfig, DiscoveryClientNode eurekaClient, HealthCheckHandler healthCheckHandler) {
        this.cloudPantheonClient = new AtomicReference();
        this.pantheonClient = eurekaClient;
        this.instanceConfig = instanceConfig;
        this.healthCheckHandler = healthCheckHandler;
    }

    public static PantheonRegistration.Builder builder(PantheonInstanceConfig instanceConfig) {
        return new PantheonRegistration.Builder(instanceConfig);
    }

    public String getServiceId() {
        return this.instanceConfig.getServiceName();
    }

    public String getHost() {
        return this.instanceConfig.getInstanceHostName();
    }

    public int getPort() {
        return this.instanceConfig.getInstancePort();
    }

    public boolean isSecure() {
        return false;
    }

    public URI getUri() {
        return DefaultServiceInstance.getUri(this);
    }

    public Map<String, String> getMetadata() {
        return this.instanceConfig.getMetadataMap();
    }

    public DiscoveryClientNode getEurekaClient() {
        if (this.cloudPantheonClient.get() == null) {
            try {
                this.cloudPantheonClient.compareAndSet(null,pantheonClient);
            } catch (Exception var2) {
                log.error("error getting PantheonClient", var2);
            }
        }

        return (DiscoveryClientNode) this.cloudPantheonClient.get();
    }

//    protected <T> T getTargetObject(Object proxy, Class<T> targetClass) throws Exception {
//        return AopUtils.isJdkDynamicProxy(proxy) ? ((Advised)proxy).getTargetSource().getTarget() : proxy;
//    }

    public PantheonInstanceConfig getInstanceConfig() {
        return this.instanceConfig;
    }

    public HealthCheckHandler getHealthCheckHandler() {
        return this.healthCheckHandler;
    }

    public void setHealthCheckHandler(HealthCheckHandler healthCheckHandler) {
        this.healthCheckHandler = healthCheckHandler;
    }

    public void setNonSecurePort(int port) {
        this.instanceConfig.setInstancePort(port);
    }

    public int getNonSecurePort() {
        return this.instanceConfig.getInstancePort();
    }


    public static class Builder {
        private final PantheonInstanceConfig instanceConfig;
        private DiscoveryClientNode pantheonClient;
        private HealthCheckHandler healthCheckHandler;
        private PantheonInstanceConfig clientConfig;
        private ApplicationEventPublisher publisher;

        Builder(PantheonInstanceConfig instanceConfig) {
            this.instanceConfig = instanceConfig;
        }


        public PantheonRegistration.Builder with(DiscoveryClientNode pantheonClient) {
            this.pantheonClient = pantheonClient;
            return this;
        }

        public PantheonRegistration.Builder with(HealthCheckHandler healthCheckHandler) {
            this.healthCheckHandler = healthCheckHandler;
            return this;
        }

        public PantheonRegistration.Builder with(PantheonInstanceConfig clientConfig, ApplicationEventPublisher publisher) {
            this.clientConfig = clientConfig;
            this.publisher = publisher;
            return this;
        }

        public PantheonRegistration build() {
            Assert.notNull(this.instanceConfig, "instanceConfig may not be null");

            if (this.pantheonClient == null) {
                Assert.notNull(this.clientConfig, "if pantheonClient is null, clientConfig may not be null");
                Assert.notNull(this.publisher, "if eurekaClient is null, ApplicationEventPublisher may not be null");
                this.pantheonClient = new DiscoveryClientNode(this.clientConfig);
            }

            return new PantheonRegistration(this.instanceConfig, this.pantheonClient, this.healthCheckHandler);
        }
    }
}
