package com.pantheon.netflix.client;

import com.pantheon.client.DiscoveryClientNode;
import com.pantheon.client.appinfo.Application;
import com.pantheon.client.appinfo.Applications;
import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.client.config.PantheonInstanceConfig;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Anthony
 * @create 2021/12/14
 * @desc Reference from spring-cloud-netflix-eureka-client：1.4.5.RELEASE
 **/
public class PantheonDiscoveryClient implements DiscoveryClient {
    public static final String DESCRIPTION = "Pantheon Discovery Client";
    private final PantheonInstanceConfig config;

    private final DiscoveryClientNode pantheonClient;

    public PantheonDiscoveryClient(PantheonInstanceConfig config, DiscoveryClientNode pantheonClient) {
        this.config = config;
        this.pantheonClient = pantheonClient;
    }

    @Override
    public String description() {
        return DESCRIPTION;
    }

    @Override
    public ServiceInstance getLocalServiceInstance() {
        return new ServiceInstance() {
            @Override
            public String getServiceId() {
                return PantheonDiscoveryClient.this.config.getServiceName();
            }

            @Override
            public String getHost() {
                return PantheonDiscoveryClient.this.config.getInstanceHostName();
            }

            @Override
            public int getPort() {
                return PantheonDiscoveryClient.this.config.getInstancePort();
            }

            @Override
            public boolean isSecure() {
                return false;
            }

            @Override
            public URI getUri() {
                return DefaultServiceInstance.getUri(this);
            }

            @Override
            public Map<String, String> getMetadata() {
                return PantheonDiscoveryClient.this.config.getMetadataMap();
            }
        };
    }

    @Override
    public List<ServiceInstance> getInstances(String serviceId) {
                List<InstanceInfo> infos = this.pantheonClient.getInstance(serviceId);
        List<ServiceInstance> instances = new ArrayList<>();
        for (InstanceInfo info : infos) {
            instances.add(new PantheonServiceInstance(info));
        }
        return instances;
    }

    public static class PantheonServiceInstance implements ServiceInstance {
        private InstanceInfo instance;

        public PantheonServiceInstance(InstanceInfo instance) {
            this.instance = instance;
        }

        public InstanceInfo getInstanceInfo() {
            return instance;
        }

        @Override
        public String getServiceId() {
            return this.instance.getAppName();
        }

        @Override
        public String getHost() {
            return this.instance.getHostName();
        }

        @Override
        public int getPort() {
            if (isSecure()) {
                return this.instance.getSecurePort();
            }
            return this.instance.getPort();
        }

        @Override
        public boolean isSecure() {
            // assume if secure is enabled, that is the default
            return false;
        }

        @Override
        public URI getUri() {
            return DefaultServiceInstance.getUri(this);
        }

        @Override
        public Map<String, String> getMetadata() {
            return this.instance.getMetadata();
        }
    }

    @Override
    public List<String> getServices() {
        Applications applications = this.pantheonClient.getApplications();
        if (applications == null) {
            return Collections.emptyList();
        }
        List<Application> registered = applications.getRegisteredApplications();
        List<String> names = new ArrayList<>();
        for (Application app : registered) {
            if (app.getInstances().isEmpty()) {
                continue;
            }
            names.add(app.getName().toLowerCase());

        }
        return names;
    }
}
