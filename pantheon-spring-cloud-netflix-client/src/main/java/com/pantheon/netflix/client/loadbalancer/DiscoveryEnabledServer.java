package com.pantheon.netflix.client.loadbalancer;

import com.netflix.loadbalancer.Server;
import com.pantheon.client.appinfo.InstanceInfo;
import com.sun.xml.internal.ws.wsdl.writer.document.PortType;

/**
 * @author Anthony
 * @create 2021/12/20
 * @desc
 **/
@SuppressWarnings({"EQ_DOESNT_OVERRIDE_EQUALS"})
public class DiscoveryEnabledServer extends Server {
    private final InstanceInfo instanceInfo;
    private final MetaInfo serviceInfo;

    public DiscoveryEnabledServer(InstanceInfo instanceInfo, boolean useSecurePort) {
        this(instanceInfo, useSecurePort, false);
    }

    public DiscoveryEnabledServer(final InstanceInfo instanceInfo, boolean useSecurePort, boolean useIpAddr) {
        super(useIpAddr ? instanceInfo.getIPAddr() : instanceInfo.getHostName(), instanceInfo.getPort());
        if (useSecurePort) {
            super.setPort(instanceInfo.getSecurePort());
        }

        this.instanceInfo = instanceInfo;
        this.serviceInfo = new MetaInfo() {
            public String getAppName() {
                return instanceInfo.getAppName();
            }

            public String getServerGroup() {
                return instanceInfo.getAppGroupName();
            }

            public String getServiceIdForDiscovery() {
                return instanceInfo.getInstanceId();
            }

            public String getInstanceId() {
                return instanceInfo.getId();
            }
        };
    }

    public InstanceInfo getInstanceInfo() {
        return this.instanceInfo;
    }

    public MetaInfo getMetaInfo() {
        return this.serviceInfo;
    }
}
