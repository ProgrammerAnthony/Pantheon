package com.pantheon.netflix.client.loadbalancer;

import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.DefaultClientConfigImpl;
import com.netflix.client.config.IClientConfig;
import com.netflix.client.config.IClientConfigKey;
import com.netflix.config.ConfigurationManager;
import com.netflix.loadbalancer.AbstractServerList;
import com.netflix.loadbalancer.DynamicServerListLoadBalancer;
import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.client.discovery.DiscoveryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Anthony
 * @create 2021/12/20
 * @desc The server list class that fetches the server information from Pantheon client.
 * ServerList is used by {@link DynamicServerListLoadBalancer}  to get server list dynamically.
 **/
public class DiscoveryEnabledNIWSServerList extends AbstractServerList<DiscoveryEnabledServer> {
    private static final Logger logger = LoggerFactory.getLogger(DiscoveryEnabledNIWSServerList.class);
    String clientName;
    String vipAddresses;
    boolean isSecure;
    boolean prioritizeVipAddressBasedServers;
    String datacenter;
    String targetRegion;
    int overridePort;
    boolean shouldUseOverridePort;
    boolean shouldUseIpAddr;
    private final Provider<DiscoveryClient> pantheonClientProvider;

    public DiscoveryEnabledNIWSServerList(String vipAddresses, Provider<DiscoveryClient> pantheonClientProvider) {
        this(createClientConfig(vipAddresses), pantheonClientProvider);
    }

    public DiscoveryEnabledNIWSServerList(IClientConfig clientConfig, Provider<DiscoveryClient> pantheonClientProvider) {
        this.isSecure = false;
        this.prioritizeVipAddressBasedServers = true;
        this.overridePort = 7001;
        this.shouldUseOverridePort = false;
        this.shouldUseIpAddr = false;
        this.pantheonClientProvider = pantheonClientProvider;
        this.initWithNiwsConfig(clientConfig);
    }

    public void initWithNiwsConfig(IClientConfig clientConfig) {
        this.clientName = clientConfig.getClientName();
        this.vipAddresses = clientConfig.resolveDeploymentContextbasedVipAddresses();
        if (this.vipAddresses == null && ConfigurationManager.getConfigInstance().getBoolean("DiscoveryEnabledNIWSServerList.failFastOnNullVip", true)) {
            throw new NullPointerException("VIP address for client " + this.clientName + " is null");
        } else {
            this.isSecure = Boolean.parseBoolean("" + clientConfig.getProperty(CommonClientConfigKey.IsSecure, "false"));
            this.prioritizeVipAddressBasedServers = Boolean.parseBoolean("" + clientConfig.getProperty(CommonClientConfigKey.PrioritizeVipAddressBasedServers, this.prioritizeVipAddressBasedServers));
            this.datacenter = ConfigurationManager.getDeploymentContext().getDeploymentDatacenter();
            this.targetRegion = (String) clientConfig.getProperty(CommonClientConfigKey.TargetRegion);
            this.shouldUseIpAddr = clientConfig.getPropertyAsBoolean(CommonClientConfigKey.UseIPAddrForServer, DefaultClientConfigImpl.DEFAULT_USEIPADDRESS_FOR_SERVER);
            if (clientConfig.getPropertyAsBoolean(CommonClientConfigKey.ForceClientPortConfiguration, false)) {
                if (this.isSecure) {
                    if (clientConfig.containsProperty(CommonClientConfigKey.SecurePort)) {
                        this.overridePort = clientConfig.getPropertyAsInteger(CommonClientConfigKey.SecurePort, 7001);
                        this.shouldUseOverridePort = true;
                    } else {
                        logger.warn(this.clientName + " set to force client port but no secure port is set, so ignoring");
                    }
                } else if (clientConfig.containsProperty(CommonClientConfigKey.Port)) {
                    this.overridePort = clientConfig.getPropertyAsInteger(CommonClientConfigKey.Port, 7001);
                    this.shouldUseOverridePort = true;
                } else {
                    logger.warn(this.clientName + " set to force client port but no port is set, so ignoring");
                }
            }

        }
    }

    public List<DiscoveryEnabledServer> getInitialListOfServers() {
        return this.obtainServersViaDiscovery();
    }

    public List<DiscoveryEnabledServer> getUpdatedListOfServers() {
        return this.obtainServersViaDiscovery();
    }

    private List<DiscoveryEnabledServer> obtainServersViaDiscovery() {
        List<DiscoveryEnabledServer> serverList = new ArrayList();
        if (this.pantheonClientProvider != null && this.pantheonClientProvider.get() != null) {
            DiscoveryClient pantheonClient = (DiscoveryClient) this.pantheonClientProvider.get();
            if (this.vipAddresses != null) {
                String[] var3 = this.vipAddresses.split(",");
                int var4 = var3.length;

                for (int var5 = 0; var5 < var4; ++var5) {
                    String vipAddress = var3[var5];
                    List<InstanceInfo> listOfInstanceInfo = pantheonClient.getInstance(vipAddress);
                    Iterator var8 = listOfInstanceInfo.iterator();

                    while (var8.hasNext()) {
                        InstanceInfo ii = (InstanceInfo) var8.next();
                        if (ii.getStatus().equals(InstanceInfo.InstanceStatus.UP)) {
                            if (this.shouldUseOverridePort) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Overriding port on client name: " + this.clientName + " to " + this.overridePort);
                                }

                                InstanceInfo copy = new InstanceInfo(ii);
                                ii = (new InstanceInfo.Builder(copy)).setPort(this.overridePort).build();
                            }

                            DiscoveryEnabledServer des = new DiscoveryEnabledServer(ii, this.isSecure, this.shouldUseIpAddr);
                            des.setZone("default");
                            serverList.add(des);
                        }
                    }

                    if (serverList.size() > 0 && this.prioritizeVipAddressBasedServers) {
                        break;
                    }
                }
            }

            return serverList;
        } else {
            logger.warn("PantheonClient has not been initialized yet, returning an empty list");
            return new ArrayList();
        }
    }

    public String getVipAddresses() {
        return this.vipAddresses;
    }

    public void setVipAddresses(String vipAddresses) {
        this.vipAddresses = vipAddresses;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("DiscoveryEnabledNIWSServerList:");
        sb.append("; clientName:").append(this.clientName);
        sb.append("; Effective vipAddresses:").append(this.vipAddresses);
        sb.append("; isSecure:").append(this.isSecure);
        sb.append("; datacenter:").append(this.datacenter);
        return sb.toString();
    }

    private static IClientConfig createClientConfig(String vipAddresses) {
        IClientConfig clientConfig = DefaultClientConfigImpl.getClientConfigWithDefaultValues();
        clientConfig.set(IClientConfigKey.Keys.DeploymentContextBasedVipAddresses, vipAddresses);
        return clientConfig;
    }
}
