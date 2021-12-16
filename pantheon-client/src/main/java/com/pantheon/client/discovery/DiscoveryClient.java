
package com.pantheon.client.discovery;


import com.pantheon.client.appinfo.Application;
import com.pantheon.client.appinfo.Applications;
import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.client.config.PantheonInstanceConfig;

import java.util.List;


public interface DiscoveryClient {

    /**
     * Returns the corresponding {@link Application} object which is basically a
     * container of all registered <code>appName</code> {@link InstanceInfo}s.
     *
     * @param appName
     * @return a {@link Application} or null if we couldn't locate any app of
     * the requested appName
     */
    Application getApplication(String appName);

    /**
     * Returns the {@link Applications} object which is basically a container of
     * all currently registered {@link Application}s.
     *
     * @return {@link Applications}
     */
    Applications getApplications();

    /**
     * return the {@link Applications} object which the service is subscribing
     *
     * @param serviceId
     * @return
     */
    Applications getSubscribeApplications(String serviceId);

    /**
     * Shuts down Pantheon Client. Also sends a deregistration request to the pantheon server.
     */
    void shutdown();

    /**
     * @return the configuration of this pantheon client
     */
    PantheonInstanceConfig getPantheonClientConfig();



    /**
     * Gets the list of instances matching the given  Address.
     *
     * @param address The  address to match the instances for.
     * @return - The list of {@link InstanceInfo} objects matching the criteria
     */
    List<InstanceInfo> getInstance(String address);


    /**
     * Return the current instance status as seen on the Pantheon server.
     * @return
     */
    InstanceInfo.InstanceStatus getInstanceRemoteStatus();
}
