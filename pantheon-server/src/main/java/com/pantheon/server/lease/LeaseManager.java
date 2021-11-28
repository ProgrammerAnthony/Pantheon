
package com.pantheon.server.lease;


/**
 * This class is responsible for creating/renewing and evicting a <em>lease</em>
 * for a particular instance.
 *
 * <p>
 * Leases determine what instances receive traffic. When there is no renewal
 * request from the client, the lease gets expired and the instances are evicted
 * out of {@link AbstractInstanceRegistry}. This is key to instances receiving traffic
 * or not.
 * <p>
 *
 * @author Karthik Ranganathan, Greg Kim
 *
 * @param <T>
 */
public interface LeaseManager<T> {

    /**
     * Assign a new {@link Lease} to the passed in {@link T}.
     *
     * @param r
     *            - T to register
     * @param leaseDuration
     *            - whether this is a replicated entry from another eureka node.
     */
    void register(T r, int leaseDuration);

    /**
     * Cancel the {@link Lease} associated with the passed in <code>appName</code>
     * and <code>id</code>.
     *
     * @param appName
     *            - unique id of the application.
     * @param id
     *            - unique id within appName.
     * @return true, if the operation was successful, false otherwise.
     */
    boolean cancel(String appName, String id);

    /**
     * Renew the {@link Lease} associated with the passed in <code>appName</code>
     * and <code>id</code>.
     *
     * @param id  - unique id within appName
     *            - whether this is a replicated entry from another ds node
     * @return whether the operation of successful
     */
    boolean renew(String appName, String id);

    /**
     * Evict {@link T}s with expired {@link Lease}(s).
     */
    void evict();
}
