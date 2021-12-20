package com.pantheon.netflix.client.ribbon.pantheon;

import com.netflix.loadbalancer.Server;
import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.netflix.client.loadbalancer.DiscoveryEnabledServer;

/**
 * @author Anthony
 * @create 2021/12/20
 * @desc
 **/
class DomainExtractingServer extends DiscoveryEnabledServer {
    private String id;

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public DomainExtractingServer(DiscoveryEnabledServer server, boolean useSecurePort, boolean useIpAddr, boolean approximateZoneFromHostname) {
        super(server.getInstanceInfo(), useSecurePort, useIpAddr);
        if (server.getInstanceInfo().getMetadata().containsKey("zone")) {
            this.setZone((String)server.getInstanceInfo().getMetadata().get("zone"));
        } else if (approximateZoneFromHostname) {
            this.setZone(ZoneUtils.extractApproximateZone(server.getHost()));
        } else {
            this.setZone(server.getZone());
        }

        this.setId(this.extractId(server));
        this.setAlive(server.isAlive());
        this.setReadyToServe(server.isReadyToServe());
    }

    private String extractId(Server server) {
        if (server instanceof DiscoveryEnabledServer) {
            DiscoveryEnabledServer enabled = (DiscoveryEnabledServer)server;
            InstanceInfo instance = enabled.getInstanceInfo();
            if (instance.getMetadata().containsKey("instanceId")) {
                return instance.getHostName() + ":" + (String)instance.getMetadata().get("instanceId");
            }
        }

        return super.getId();
    }
}
