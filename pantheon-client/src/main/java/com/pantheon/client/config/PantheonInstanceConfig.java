package com.pantheon.client.config;

import java.util.List;
import java.util.Map;

/**
 * @author Anthony
 * @create 2021/11/17
 * @desc Configuration information required by the instance to register with Pantheon server.
 **/
public interface PantheonInstanceConfig {

    /**
     * Get the name of the Service to be registered with pantheon.
     */
    String getServiceName();

    String getInstanceId();

    /**
     * default localhost name
     */
    String getInstanceHostName();

    /**
     * default localhost ip address
     */
    String getInstanceIpAddress();

    Integer getLeaseRenewalIntervalInSeconds();

    Integer getLeaseExpirationDurationInSeconds();

    /**
     * instances connect one of these servers
     *
     * @return
     */
    List<String> getServerList();

    Integer getInstancePort();

    Integer setInstancePort(Integer port);

    /**
     * should fetch registry ,default true
     * @return
     */
    boolean shouldFetchRegistry();

    boolean shouldFilterOnlyUpInstances();

    /**
     * Indicates how often(in seconds) to fetch the registry information from
     * the  server.
     *
     * @return the fetch interval in seconds.
     */
    int getRegistryFetchIntervalSeconds();

    Map<String, String> getMetadataMap();
}
