package com.pantheon.netflix.client;

import com.pantheon.client.PantheonClient;
import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.client.config.PantheonInstanceConfig;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Anthony
 * @create 2021/12/14
 * @desc
 **/
public class PantheonDiscoveryClient implements DiscoveryClient {
    public static final String DESCRIPTION = "Pantheon Discovery Client";
    private final PantheonInstanceConfig config;

    private final PantheonClient pantheonClient;

    public PantheonDiscoveryClient(PantheonInstanceConfig config, PantheonClient pantheonClient) {
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
    public List<ServiceInstance> getInstances(String s) {
return null;
        //        List<InstanceInfo> infos = this.pantheonClient.get(serviceId,
//                false);
//        List<ServiceInstance> instances = new ArrayList<>();
//        for (InstanceInfo info : infos) {
//            instances.add(new ServiceInstance(info));
//        }
//        return instances;
    }

    @Override
    public List<String> getServices() {
        return null;
    }
}
