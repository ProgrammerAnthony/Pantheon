package com.pantheon.client.appinfo;

import com.pantheon.remoting.protocol.RemotingSerializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Anthony
 * @create 2021/11/28
 * @desc The class that holds information required for registration
 */
public class InstanceInfo extends RemotingSerializable {

    private String hostName;


    private LeaseInfo leaseInfo;
    private InstanceStatus instanceStatus = InstanceStatus.UP;


    public static class PortWrapper {
        private final boolean enabled;
        private final int port;

        public PortWrapper(boolean enabled, int port) {
            this.enabled = enabled;
            this.port = port;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public int getPort() {
            return port;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(InstanceInfo.class);

    public static final int DEFAULT_PORT = 7001;
    public static final int DEFAULT_SECURE_PORT = 7002;
    public static final int DEFAULT_COUNTRY_ID = 1; // US

    // The (fixed) instanceId for this instanceInfo. This should be unique within the scope of the appName.
    private volatile String instanceId;

    private volatile String appName;

    private volatile String appGroupName;

    private volatile String ipAddr;


    private volatile int port = DEFAULT_PORT;
    private volatile int securePort = DEFAULT_SECURE_PORT;

    private volatile InstanceStatus status = InstanceStatus.UP;
    private volatile InstanceStatus overriddenstatus = InstanceStatus.UNKNOWN;
    private volatile boolean isInstanceInfoDirty = false;
    private volatile Long lastUpdatedTimestamp = System.currentTimeMillis();
    private volatile Long lastDirtyTimestamp = System.currentTimeMillis();
    private volatile ActionType actionType;


    public enum InstanceStatus {
        UP, // Ready to receive traffic
        DOWN, // Do not send traffic- healthcheck callback failed
        STARTING, // Just about starting- initializations to be done - do not
        // send traffic
        OUT_OF_SERVICE, // Intentionally shutdown for traffic
        UNKNOWN;

        public static InstanceStatus toEnum(String s) {
            for (InstanceStatus e : InstanceStatus.values()) {
                if (e.name().equalsIgnoreCase(s)) {
                    return e;
                }
            }
            return UNKNOWN;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((getId() == null) ? 0 : getId().hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        InstanceInfo other = (InstanceInfo) obj;
        if (getId() == null) {
            if (other.getId() != null) {
                return false;
            }
        } else if (!getId().equals(other.getId())) {
            return false;
        }
        return true;
    }


    public static final class Builder {
        private static final String COLON = ":";
        private static final String HTTPS_PROTOCOL = "https://";
        private static final String HTTP_PROTOCOL = "http://";

        private InstanceInfo result;

        public Builder setInstanceId(String instanceId) {
            result.instanceId = instanceId;
            return this;
        }

        public static Builder newBuilder() {
            return new Builder(new InstanceInfo());
        }

        public Builder(InstanceInfo result) {
            this.result = result;
        }

        /**
         * Set the application name of the instance.This is mostly used in
         * querying of instances.
         *
         * @param appName the application name.
         * @return the instance info builder.
         */
        public Builder setAppName(String appName) {
            result.appName = appName;
            return this;
        }

        public Builder setAppGroupName(String appGroupName) {
            result.appGroupName = appGroupName;
            return this;
        }

        /**
         * Sets the fully qualified hostname of this running instance.This is
         * mostly used in constructing the {@link java.net.URL} for communicating with
         * the instance.
         *
         * @param hostName the host name of the instance.
         * @return the {@link InstanceInfo} builder.
         */
        public Builder setHostName(String hostName) {
            result.hostName=hostName;
            return this;
        }

        /**
         * Sets the status of the instances.If the status is UP, that is when
         * the instance is ready to service requests.
         *
         * @param status the {@link InstanceStatus} of the instance.
         * @return the {@link InstanceInfo} builder.
         */
        public Builder setStatus(InstanceStatus status) {
            result.status = status;
            return this;
        }

        /**
         * Sets the status overridden by some other external process.This is
         * mostly used in putting an instance out of service to block traffic to
         * it.
         *
         * @param status the overridden {@link InstanceStatus} of the instance.
         * @return @return the {@link InstanceInfo} builder.
         */
        public Builder setOverriddenStatus(InstanceStatus status) {
            result.overriddenstatus = status;
            return this;
        }

        /**
         * Sets the ip address of this running instance.
         *
         * @param ip the ip address of the instance.
         * @return the {@link InstanceInfo} builder.
         */
        public Builder setIPAddr(String ip) {
            result.ipAddr = ip;
            return this;
        }

        /**
         * Sets the port number that is used to service requests.
         *
         * @param port the port number that is used to service requests.
         * @return the {@link InstanceInfo} builder.
         */
        public Builder setPort(int port) {
            result.port = port;
            return this;
        }

        /**
         * Sets the secure port that is used to service requests.
         *
         * @param port the secure port that is used to service requests.
         * @return the {@link InstanceInfo} builder.
         */
        public Builder setSecurePort(int port) {
            result.securePort = port;
            return this;
        }

        /**
         * Set the instance lease information.
         *
         * @param info the lease information for this instance.
         */
        public Builder setLeaseInfo(LeaseInfo info) {
            result.leaseInfo = info;
            return this;
        }


        /**
         * Returns the encapsulated instance info even if it is not built fully.
         *
         * @return the existing information about the instance.
         */
        public InstanceInfo getRawInstance() {
            return result;
        }

        /**
         * Build the {@link InstanceInfo} object.
         *
         * @return the {@link InstanceInfo} that was built based on the
         * information supplied.
         */
        public InstanceInfo build() {
            if (!isInitialized()) {
                throw new IllegalStateException("name is required!");
            }
            return result;
        }

        public boolean isInitialized() {
            return (result.appName != null);
        }


        public Builder setLastUpdatedTimestamp(long lastUpdatedTimestamp) {
            result.lastUpdatedTimestamp = lastUpdatedTimestamp;
            return this;
        }

        public Builder setLastDirtyTimestamp(long lastDirtyTimestamp) {
            result.lastDirtyTimestamp = lastDirtyTimestamp;
            return this;
        }

        public Builder setActionType(ActionType actionType) {
            result.actionType = actionType;
            return this;
        }


    }

    /**
     * @return the raw instanceId. For compatibility, prefer to use {@link #getId()}
     */
    public String getInstanceId() {
        return instanceId;
    }

    /**
     * Return the name of the application registering with discovery.
     *
     * @return the string denoting the application name.
     */
    public String getAppName() {
        return appName;
    }

    public String getAppGroupName() {
        return appGroupName;
    }


    /**
     * Return the default network address to connect to this instance.
     */
    public String getHostName() {
        return hostName;
    }


    /**
     * Returns the unique id of the instance.
     * (Note) now that id is set at creation time within the instanceProvider, why do the other checks?
     * This is still necessary for backwards compatibility when upgrading in a deployment with multiple
     * client versions (some with the change, some without).
     *
     * @return the unique id.
     */
    public String getId() {
        if (instanceId != null && !instanceId.isEmpty()) {
            return instanceId;
        }
        return hostName;
    }

    /**
     * Returns the ip address of the instance.
     *
     * @return - the ip address, in AWS scenario it is a private IP.
     */
    public String getIPAddr() {
        return ipAddr;
    }

    /**
     * Returns the port number that is used for servicing requests.
     *
     * @return - the non-secure port number.
     */
    public int getPort() {
        return port;
    }

    /**
     * Returns the status of the instance.
     *
     * @return the status indicating whether the instance can handle requests.
     */
    public InstanceStatus getStatus() {
        return status;
    }

    /**
     * Returns the overridden status if any of the instance.
     *
     * @return the status indicating whether an external process has changed the
     * status.
     */
    public InstanceStatus getOverriddenStatus() {
        return overriddenstatus;
    }

    /**
     * Returns the lease information regarding when it expires.
     *
     * @return the lease information of this instance.
     */
    public LeaseInfo getLeaseInfo() {
        return leaseInfo;
    }

    /**
     * Sets the lease information regarding when it expires.
     *
     * @param info the lease information of this instance.
     */
    public void setLeaseInfo(LeaseInfo info) {
        leaseInfo = info;
    }



    /**
     * Returns the time elapsed since epoch since the instance status has been
     * last updated.
     *
     * @return the time elapsed since epoch since the instance has been last
     * updated.
     */
    public long getLastUpdatedTimestamp() {
        return lastUpdatedTimestamp;
    }

    /**
     * Set the update time for this instance when the status was update.
     */
    public void setLastUpdatedTimestamp() {
        this.lastUpdatedTimestamp = System.currentTimeMillis();
    }


    /**
     * Gets the last time stamp when this instance was touched.
     *
     * @return last timestamp when this instance was touched.
     */
    public Long getLastDirtyTimestamp() {
        return lastDirtyTimestamp;
    }

    /**
     * Set the time indicating that the instance was touched.
     *
     * @param lastDirtyTimestamp time when the instance was touched.
     */
    public void setLastDirtyTimestamp(Long lastDirtyTimestamp) {
        this.lastDirtyTimestamp = lastDirtyTimestamp;
    }

    /**
     * Set the status for this instance.
     *
     * @param status status for this instance.
     * @return the prev status if a different status from the current was set, null otherwise
     */
    public synchronized InstanceStatus setStatus(InstanceStatus status) {
        if (this.status != status) {
            InstanceStatus prev = this.status;
            this.status = status;
            setIsDirty();
            return prev;
        }
        return null;
    }

    /**
     * Set the status for this instance without updating the dirty timestamp.
     *
     * @param status status for this instance.
     */
    public synchronized void setStatusWithoutDirty(InstanceStatus status) {
        if (this.status != status) {
            this.status = status;
        }
    }

    /**
     * Sets the overridden status for this instance.Normally set by an external
     * process to disable instance from taking traffic.
     *
     * @param status overridden status for this instance.
     */
    public synchronized void setOverriddenStatus(InstanceStatus status) {
        if (this.overriddenstatus != status) {
            this.overriddenstatus = status;
        }
    }

    /**
     * Returns whether any state changed so that client side can
     * check whether to retransmit info or not on the next heartbeat.
     *
     * @return true if the {@link InstanceInfo} is dirty, false otherwise.
     */
    public boolean isDirty() {
        return isInstanceInfoDirty;
    }

    /**
     * @return the lastDirtyTimestamp if is dirty, null otherwise.
     */
    public synchronized Long isDirtyWithTime() {
        if (isInstanceInfoDirty) {
            return lastDirtyTimestamp;
        } else {
            return null;
        }
    }

    /**
     * @param isDirty true if dirty, false otherwise.
     * @deprecated use {@link #setIsDirty()} and {@link #unsetIsDirty(long)} to set and unset
     * <p>
     * Sets the dirty flag so that the instance information can be carried to
     * the discovery server on the next heartbeat.
     */
    @Deprecated
    public synchronized void setIsDirty(boolean isDirty) {
        if (isDirty) {
            setIsDirty();
        } else {
            isInstanceInfoDirty = false;
            // else don't update lastDirtyTimestamp as we are setting isDirty to false
        }
    }

    /**
     * Sets the dirty flag so that the instance information can be carried to
     * the discovery server on the next heartbeat.
     */
    public synchronized void setIsDirty() {
        isInstanceInfoDirty = true;
        lastDirtyTimestamp = System.currentTimeMillis();
    }

    /**
     * Set the dirty flag, and also return the timestamp of the isDirty event
     *
     * @return the timestamp when the isDirty flag is set
     */
    public synchronized long setIsDirtyWithTime() {
        setIsDirty();
        return lastDirtyTimestamp;
    }


    /**
     * Unset the dirty flag if the unsetDirtyTimestamp matches the lastDirtyTimestamp. No-op if
     * lastDirtyTimestamp > unsetDirtyTimestamp
     *
     * @param unsetDirtyTimestamp the expected lastDirtyTimestamp to unset.
     */
    public synchronized void unsetIsDirty(long unsetDirtyTimestamp) {
        if (lastDirtyTimestamp <= unsetDirtyTimestamp) {
            isInstanceInfoDirty = false;
        } else {
        }
    }

    /**
     * Returns the type of action done on the instance in the server.Primarily
     * used for updating deltas in the client
     * instance.
     *
     * @return action type done on the instance.
     */
    public ActionType getActionType() {
        return actionType;
    }

    /**
     * Set the action type performed on this instance in the server.
     *
     * @param actionType action type done on the instance.
     */
    public void setActionType(ActionType actionType) {
        this.actionType = actionType;
    }


    public enum ActionType {
        ADDED, // Added in the discovery server
        MODIFIED, // Changed in the discovery server
        DELETED
        // Deleted from the discovery server
    }
}
