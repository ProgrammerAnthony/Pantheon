package com.pantheon.client.config;

import java.util.List;

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
     * @return
     */
    List<String> getServerList();
}
