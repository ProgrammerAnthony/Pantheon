package com.pantheon.client.config;

/**
 * @author Anthony
 * @create 2021/11/17
 * @desc Configuration information required by the instance to register with Pantheon server.
 **/
public interface PantheonInstanceConfig {
    /**
     * Get the unique Id (within the scope of the appName) of this instance to be registered with pantheon.
     */
    String getInstanceId();

    /**
     * Get the name of the application to be registered with pantheon.
     */
    String getAppname();

    String getHostName(boolean refresh);

    String getIpAddress();

    int getLeaseRenewalIntervalInSeconds();

    int getLeaseExpirationDurationInSeconds();
}
