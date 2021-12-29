package com.pantheon.netflix.client.ribbon;

import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.IClientConfig;
import com.netflix.config.ConfigurationManager;
import com.netflix.config.DeploymentContext;
import com.netflix.loadbalancer.IPing;
import com.netflix.loadbalancer.ServerList;
import com.pantheon.client.config.PantheonInstanceConfig;
import com.pantheon.client.discovery.DiscoveryClient;
import com.pantheon.netflix.client.loadbalancer.DiscoveryEnabledNIWSServerList;
import com.pantheon.netflix.client.loadbalancer.NIWSDiscoveryPing;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.netflix.ribbon.PropertiesFactory;
import org.springframework.cloud.netflix.ribbon.RibbonUtils;
import org.springframework.cloud.netflix.ribbon.ServerIntrospector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.inject.Provider;

/**
 * @author Anthony
 * @create 2021/12/20
 * @desc Preprocessor that configures defaults for pantheon-discovered ribbon clients.
 * Such as: @zone, NIWSServerListClassName, DeploymentContextBasedVipAddresses, NFLoadBalancerRuleClassName, NIWSServerListFilterClassName and more
 **/
@Configuration
public class PantheonRibbonClientConfiguration {
    private static final Log log = LogFactory.getLog(PantheonRibbonClientConfiguration.class);
    @Value("${ribbon.pantheon.approximateZoneFromHostname:false}")
    private boolean approximateZoneFromHostname = false;
    @Value("${ribbon.client.name}")
    private String serviceId = "client";

    @Autowired(
            required = false
    )
    private PantheonInstanceConfig pantheonConfig;
    @Autowired
    private PropertiesFactory propertiesFactory;

    public PantheonRibbonClientConfiguration() {
    }

    public PantheonRibbonClientConfiguration( PantheonInstanceConfig pantheonConfig, String serviceId,boolean approximateZoneFromHostname) {
        this.serviceId = serviceId;
        this.pantheonConfig = pantheonConfig;
        this.approximateZoneFromHostname = approximateZoneFromHostname;
    }

    @Bean
    @ConditionalOnMissingBean
    public IPing ribbonPing(IClientConfig config) {
        if (this.propertiesFactory.isSet(IPing.class, this.serviceId)) {
            return (IPing)this.propertiesFactory.get(IPing.class, config, this.serviceId);
        } else {
            NIWSDiscoveryPing ping = new NIWSDiscoveryPing();
            ping.initWithNiwsConfig(config);
            return ping;
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public ServerList<?> ribbonServerList(IClientConfig config, Provider<DiscoveryClient> pantheonClientProvider) {
        if (this.propertiesFactory.isSet(ServerList.class, this.serviceId)) {
            return (ServerList)this.propertiesFactory.get(ServerList.class, config, this.serviceId);
        } else {
            DiscoveryEnabledNIWSServerList discoveryServerList = new DiscoveryEnabledNIWSServerList(config, pantheonClientProvider);
            DomainExtractingServerList serverList = new DomainExtractingServerList(discoveryServerList, config, this.approximateZoneFromHostname);
            return serverList;
        }
    }

    @Bean
    public ServerIntrospector serverIntrospector() {
        return new PantheonServerIntrospector();
    }

    @PostConstruct
    public void preprocess() {
        String zone = ConfigurationManager.getDeploymentContext().getValue(DeploymentContext.ContextKey.zone);
        if (this.pantheonConfig != null && StringUtils.isEmpty(zone)) {
            String availabilityZone;
            if (this.approximateZoneFromHostname && this.pantheonConfig != null) {
                availabilityZone = ZoneUtils.extractApproximateZone(this.pantheonConfig.getInstanceHostName());
                log.debug("Setting Zone To " + availabilityZone);
                ConfigurationManager.getDeploymentContext().setValue(DeploymentContext.ContextKey.zone, availabilityZone);
            } else {
                availabilityZone = this.pantheonConfig == null ? null : (String)this.pantheonConfig.getMetadataMap().get("zone");
                if (availabilityZone == null) {
//                    String[] zones = this.pantheonConfig.getAvailabilityZones(this.pantheonConfig.getRegion());
//                    availabilityZone = zones != null && zones.length > 0 ? zones[0] : null;
                }

                if (availabilityZone != null) {
                    ConfigurationManager.getDeploymentContext().setValue(DeploymentContext.ContextKey.zone, availabilityZone);
                }
            }
        }

        RibbonUtils.setRibbonProperty(this.serviceId, CommonClientConfigKey.DeploymentContextBasedVipAddresses.key(), this.serviceId);
        RibbonUtils.setRibbonProperty(this.serviceId, CommonClientConfigKey.EnableZoneAffinity.key(), "true");
    }
}

