package com.pantheon.netflix.client.loadbalancer;

import com.pantheon.client.ClientManager;
import com.pantheon.client.DiscoveryClientNode;
import com.pantheon.client.config.DefaultInstanceConfig;
import com.pantheon.client.discovery.DiscoveryClient;

import javax.inject.Provider;

/**
 * @author Anthony
 * @create 2021/12/20
 * @desc
 **/

class LegacyEurekaClientProvider implements Provider<DiscoveryClient> {
    private volatile DiscoveryClientNode pantheonClient;

    LegacyEurekaClientProvider() {
    }

    public synchronized DiscoveryClient get() {
        if (this.pantheonClient == null) {
            this.pantheonClient = ClientManager.getInstance().getOrCreateClientNode(DefaultInstanceConfig.getInstance());
        }

        return this.pantheonClient;
    }
}
