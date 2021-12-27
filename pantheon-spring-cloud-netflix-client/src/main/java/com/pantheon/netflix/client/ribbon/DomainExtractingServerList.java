package com.pantheon.netflix.client.ribbon;

import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.ServerList;
import com.pantheon.netflix.client.loadbalancer.DiscoveryEnabledNIWSServerList;
import com.pantheon.netflix.client.loadbalancer.DiscoveryEnabledServer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Anthony
 * @create 2021/12/20
 * @desc {@link #getInitialListOfServers()} and  {@link #getUpdatedListOfServers()} use {@link DiscoveryEnabledNIWSServerList} to load server list dynamically
 **/
public class DomainExtractingServerList implements ServerList<DiscoveryEnabledServer> {
    private ServerList<DiscoveryEnabledServer> list;
    private IClientConfig clientConfig;
    private boolean approximateZoneFromHostname;

    public DomainExtractingServerList(ServerList<DiscoveryEnabledServer> list, IClientConfig clientConfig, boolean approximateZoneFromHostname) {
        this.list = list;
        this.clientConfig = clientConfig;
        this.approximateZoneFromHostname = approximateZoneFromHostname;
    }

    public List<DiscoveryEnabledServer> getInitialListOfServers() {
        List<DiscoveryEnabledServer> servers = this.setZones(this.list.getInitialListOfServers());
        return servers;
    }

    public List<DiscoveryEnabledServer> getUpdatedListOfServers() {
        List<DiscoveryEnabledServer> servers = this.setZones(this.list.getUpdatedListOfServers());
        return servers;
    }

    private List<DiscoveryEnabledServer> setZones(List<DiscoveryEnabledServer> servers) {
        List<DiscoveryEnabledServer> result = new ArrayList();
        boolean isSecure = this.clientConfig.getPropertyAsBoolean(CommonClientConfigKey.IsSecure, Boolean.TRUE);
        boolean shouldUseIpAddr = this.clientConfig.getPropertyAsBoolean(CommonClientConfigKey.UseIPAddrForServer, Boolean.FALSE);
        Iterator var5 = servers.iterator();

        while (var5.hasNext()) {
            DiscoveryEnabledServer server = (DiscoveryEnabledServer) var5.next();
            result.add(new DomainExtractingServer(server, isSecure, shouldUseIpAddr, this.approximateZoneFromHostname));
        }

        return result;
    }
}
