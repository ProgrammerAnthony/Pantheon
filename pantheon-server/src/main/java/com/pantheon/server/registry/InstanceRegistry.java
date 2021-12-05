package com.pantheon.server.registry;

import com.pantheon.client.appinfo.Application;
import com.pantheon.client.appinfo.Applications;
import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.server.config.PantheonServerConfig;
import com.pantheon.server.lease.LeaseManager;


/**
 * @author Anthony
 * @create 2021/11/28
 * @desc the class that load and update data from Server Side cache
 */
public interface InstanceRegistry {

    void register(final InstanceInfo info);

    /**
     * Registers a new instance with a given duration.
     *
     * @see LeaseManager#register(java.lang.Object, int)
     */
    void register(InstanceInfo registrant, int leaseDuration);


    /**
     * Get the registry information about all {@link Applications}.
     */
    Applications getApplications();


    /**
     * Get the registry information about the delta changes. The deltas are
     * cached for a window specified by
     * {@link PantheonServerConfig#getRetentionTimeInMSInDeltaQueue()}. Subsequent
     * requests for delta information may return the same information and client
     * must make sure this does not adversely affect them.
     *
     * @return all application deltas.
     */
    Applications getApplicationDeltas();

    InstanceInfo getInstanceByAppAndId(String appName, String id);

    /**
     * Marks the given instance of the given app name as renewed
     *
     * @see LeaseManager#renew(java.lang.String, java.lang.String)
     * @return error response when failed
     */
    String renew(String appName, String id);

    /**
     * Updates the status of an instance. Normally happens to put an instance
     * between {@link InstanceInfo.InstanceStatus#OUT_OF_SERVICE} and
     * {@link InstanceInfo.InstanceStatus#UP} to put the instance in and out of traffic.
     *
     * @param appName            the application name of the instance.
     * @param id                 the unique identifier of the instance.
     * @param newStatus          the new {@link InstanceInfo.InstanceStatus}.
     * @param lastDirtyTimestamp last timestamp when this instance information was updated.
     * @return true if the status was successfully updated, false otherwise.
     */
    boolean statusUpdate(String appName, String id,
                         InstanceInfo.InstanceStatus newStatus, String lastDirtyTimestamp);

    void storeOverriddenStatusIfRequired(String appName, String id, InstanceInfo.InstanceStatus overriddenStatus);

    /**
     * Removes status override for a give instance.
     *
     * @param appName            the application name of the instance.
     * @param id                 the unique identifier of the instance.
     * @param newStatus          the new {@link InstanceInfo.InstanceStatus}.
     * @param lastDirtyTimestamp last timestamp when this instance information was updated.
     * @return true if the status was successfully updated, false otherwise.
     */
    boolean deleteStatusOverride(String appName, String id,
                                 InstanceInfo.InstanceStatus newStatus,
                                 String lastDirtyTimestamp);

    /**
     * Cancels the registration of an instance.
     *
     * <p>
     * This is normally invoked by a client when it shuts down informing the
     * server to remove the instance from traffic.
     * </p>
     *
     * @param appName the application name of the application.
     * @param id      the unique identifier of the instance.
     * @return true if the instance was removed from the {@link InstanceRegistryImpl} successfully, false otherwise.
     */
    boolean cancel(String appName, String id);

    Application getApplication(String appName);


    ResponseCache getResponseCache();


}
