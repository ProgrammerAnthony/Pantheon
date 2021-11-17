package com.pantheon.client.config;

import com.netflix.config.DynamicPropertyFactory;
import com.pantheon.client.utils.Archaius1Utils;

/**
 * @author Anthony
 * @create 2021/11/17
 * @desc
 **/
public class PropertiesInstanceConfig extends AbstractInstanceConfig implements PantheonInstanceConfig {
    public static final String CONFIG_FILE_NAME = "pantheon-client";
    static final String LEASE_RENEWAL_INTERVAL_KEY = "lease.renewalInterval";
    static final String LEASE_EXPIRATION_DURATION_KEY = "lease.duration";
    public static final String DEFAULT_CONFIG_NAMESPACE = "pantheon";
    protected final String namespace;

    protected final DynamicPropertyFactory configInstance;

    public PropertiesInstanceConfig(){
        this(DEFAULT_CONFIG_NAMESPACE);
    }
    public PropertiesInstanceConfig(String namespace){
        this.namespace = namespace.endsWith(".")
                ? namespace
                : namespace + ".";
        this.configInstance = Archaius1Utils.initConfig(CONFIG_FILE_NAME);
    }

    @Override
    public String getInstanceId() {
        return null;
    }

    @Override
    public String getAppname() {
        return null;
    }

    @Override
    public int getLeaseRenewalIntervalInSeconds() {
        return configInstance.getIntProperty(namespace + LEASE_RENEWAL_INTERVAL_KEY,
                super.getLeaseRenewalIntervalInSeconds()).get();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.netflix.appinfo.AbstractInstanceConfig#
     * getLeaseExpirationDurationInSeconds()
     */
    @Override
    public int getLeaseExpirationDurationInSeconds() {
        return configInstance.getIntProperty(namespace + LEASE_EXPIRATION_DURATION_KEY,
                super.getLeaseExpirationDurationInSeconds()).get();
    }
}
