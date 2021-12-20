package com.pantheon.netflix.client.ribbon.pantheon;

import com.netflix.loadbalancer.Server;
import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.netflix.client.loadbalancer.DiscoveryEnabledServer;
import com.sun.xml.internal.ws.wsdl.writer.document.PortType;
import org.springframework.cloud.netflix.ribbon.DefaultServerIntrospector;

import java.util.Map;

/**
 * @author Anthony
 * @create 2021/12/20
 * @desc
 **/
public class PantheonServerIntrospector extends DefaultServerIntrospector {
    public PantheonServerIntrospector() {
    }

    public boolean isSecure(Server server) {
        if (server instanceof DiscoveryEnabledServer) {
            return false;
//            DiscoveryEnabledServer discoveryServer = (DiscoveryEnabledServer)server;
//            return discoveryServer.getInstanceInfo().isPortEnabled( InstanceInfo.PortType.SECURE);
        } else {
            return super.isSecure(server);
        }
    }

    public Map<String, String> getMetadata(Server server) {
        if (server instanceof DiscoveryEnabledServer) {
            DiscoveryEnabledServer discoveryServer = (DiscoveryEnabledServer) server;
            return discoveryServer.getInstanceInfo().getMetadata();
        } else {
            return super.getMetadata(server);
        }
    }
}
