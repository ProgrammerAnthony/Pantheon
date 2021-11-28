package com.pantheon.client.appinfo;

import com.pantheon.remoting.protocol.RemotingSerializable;

/**
 * @author Anthony
 * @create 2021/11/28
 * @desc Represents the <em>lease</em> information with <em>Pantheon</em>.
 */
public class LeaseInfo extends RemotingSerializable {

    public static final int DEFAULT_LEASE_RENEWAL_INTERVAL = 3;
    public static final int DEFAULT_LEASE_DURATION = 5;

    // Client settings
    private int renewalIntervalInSecs = DEFAULT_LEASE_RENEWAL_INTERVAL;
    private int durationInSecs = DEFAULT_LEASE_DURATION;

    // Server populated
    private long registrationTimestamp;
    private long lastRenewalTimestamp;
    private long evictionTimestamp;
    private long serviceUpTimestamp;

    public static final class Builder {

        private LeaseInfo result;

        private Builder() {
            result = new LeaseInfo();
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        /**
         * Sets the registration timestamp.
         *
         * @param ts
         *            time when the lease was first registered.
         * @return the {@link LeaseInfo} builder.
         */
        public Builder setRegistrationTimestamp(long ts) {
            result.registrationTimestamp = ts;
            return this;
        }

        /**
         * Sets the last renewal timestamp of lease.
         *
         * @param ts
         *            time when the lease was last renewed.
         * @return the {@link LeaseInfo} builder.
         */
        public Builder setRenewalTimestamp(long ts) {
            result.lastRenewalTimestamp = ts;
            return this;
        }

        /**
         * Sets the de-registration timestamp.
         *
         * @param ts
         *            time when the lease was removed.
         * @return the {@link LeaseInfo} builder.
         */
        public Builder setEvictionTimestamp(long ts) {
            result.evictionTimestamp = ts;
            return this;
        }

        /**
         * Sets the service UP timestamp.
         *
         * @param ts
         *            time when the leased service marked as UP.
         * @return the {@link LeaseInfo} builder.
         */
        public Builder setServiceUpTimestamp(long ts) {
            result.serviceUpTimestamp = ts;
            return this;
        }

        /**
         * Sets the client specified setting for eviction (e.g. how long to wait
         * without renewal event).
         *
         * @param d
         *            time in seconds after which the lease would expire without
         *            renewa.
         * @return the {@link LeaseInfo} builder.
         */
        public Builder setDurationInSecs(int d) {
            if (d <= 0) {
                result.durationInSecs = DEFAULT_LEASE_DURATION;
            } else {
                result.durationInSecs = d;
            }
            return this;
        }

        /**
         * Sets the client specified setting for renew interval.
         *
         * @param i
         *            the time interval with which the renewals will be renewed.
         * @return the {@link LeaseInfo} builder.
         */
        public Builder setRenewalIntervalInSecs(int i) {
            if (i <= 0) {
                result.renewalIntervalInSecs = DEFAULT_LEASE_RENEWAL_INTERVAL;
            } else {
                result.renewalIntervalInSecs = i;
            }
            return this;
        }

        /**
         * Build the {@link InstanceInfo}.
         *
         * @return the {@link LeaseInfo} information built based on the supplied
         *         information.
         */
        public LeaseInfo build() {
            return result;
        }
    }

    private LeaseInfo() {
    }


    public LeaseInfo(int renewalIntervalInSecs,
                      int durationInSecs,
                     long registrationTimestamp,
                     Long lastRenewalTimestamp,
                      long lastRenewalTimestampLegacy,
                    long evictionTimestamp,
                    long serviceUpTimestamp) {
        this.renewalIntervalInSecs = renewalIntervalInSecs;
        this.durationInSecs = durationInSecs;
        this.registrationTimestamp = registrationTimestamp;
        this.evictionTimestamp = evictionTimestamp;
        this.serviceUpTimestamp = serviceUpTimestamp;

        if (lastRenewalTimestamp == null) {
            this.lastRenewalTimestamp = lastRenewalTimestampLegacy;
        } else {
            this.lastRenewalTimestamp = lastRenewalTimestamp;
        }
    }

    /**
     * Returns the registration timestamp.
     *
     * @return time in milliseconds since epoch.
     */
    public long getRegistrationTimestamp() {
        return registrationTimestamp;
    }

    /**
     * Returns the last renewal timestamp of lease.
     *
     * @return time in milliseconds since epoch.
     */
    public long getRenewalTimestamp() {
        return lastRenewalTimestamp;
    }

    /**
     * Returns the de-registration timestamp.
     *
     * @return time in milliseconds since epoch.
     */
    public long getEvictionTimestamp() {
        return evictionTimestamp;
    }

    /**
     * Returns the service UP timestamp.
     *
     * @return time in milliseconds since epoch.
     */
    public long getServiceUpTimestamp() {
        return serviceUpTimestamp;
    }

    /**
     * Returns client specified setting for renew interval.
     *
     * @return time in milliseconds since epoch.
     */
    public int getRenewalIntervalInSecs() {
        return renewalIntervalInSecs;
    }

    /**
     * Returns client specified setting for eviction (e.g. how long to wait w/o
     * renewal event)
     *
     * @return time in milliseconds since epoch.
     */
    public int getDurationInSecs() {
        return durationInSecs;
    }
}
