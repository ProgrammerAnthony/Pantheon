package com.pantheon.netflix.client.loadbalancer;

import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.AbstractLoadBalancerPing;
import com.netflix.loadbalancer.BaseLoadBalancer;
import com.netflix.loadbalancer.Server;
import com.pantheon.client.appinfo.InstanceInfo;

/**
 * @author Anthony
 * @create 2021/12/20
 * @desc
 **/
public class NIWSDiscoveryPing extends AbstractLoadBalancerPing {
    BaseLoadBalancer lb = null;

    public NIWSDiscoveryPing() {
    }

    public BaseLoadBalancer getLb() {
        return this.lb;
    }

    public void setLb(BaseLoadBalancer lb) {
        this.lb = lb;
    }

    public boolean isAlive(Server server) {
        boolean isAlive = true;
        if (server != null && server instanceof DiscoveryEnabledServer) {
            DiscoveryEnabledServer dServer = (DiscoveryEnabledServer)server;
            InstanceInfo instanceInfo = dServer.getInstanceInfo();
            if (instanceInfo != null) {
                InstanceInfo.InstanceStatus status = instanceInfo.getStatus();
                if (status != null) {
                    isAlive = status.equals(InstanceInfo.InstanceStatus.UP);
                }
            }
        }

        return isAlive;
    }

    public void initWithNiwsConfig(IClientConfig clientConfig) {
    }
}
