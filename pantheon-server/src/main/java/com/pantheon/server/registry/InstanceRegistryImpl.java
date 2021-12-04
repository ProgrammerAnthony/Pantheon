package com.pantheon.server.registry;

import com.google.common.cache.CacheBuilder;
import com.pantheon.client.appinfo.Application;
import com.pantheon.client.appinfo.Applications;
import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.client.appinfo.LeaseInfo;
import com.pantheon.server.config.CachedPantheonServerConfig;
import com.pantheon.server.config.PantheonServerConfig;
import com.pantheon.server.lease.Lease;
import com.pantheon.server.lease.LeaseManager;
import com.pantheon.server.rule.DownOrStartingRule;
import com.pantheon.server.rule.FirstMatchWinsCompositeRule;
import com.pantheon.server.rule.InstanceStatusOverrideRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Anthony
 * @create 2021/11/28
 * @desc  the class that load and update data from Server Side cache
 */
public class InstanceRegistryImpl implements InstanceRegistry{
    private static final Logger logger = LoggerFactory.getLogger(InstanceRegistryImpl.class);
    private final ConcurrentHashMap<String/*appName*/, Map<String/*instanceId*/, Lease<InstanceInfo>>> registry
            = new ConcurrentHashMap<String, Map<String, Lease<InstanceInfo>>>();

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock read = readWriteLock.readLock();
    private final Lock write = readWriteLock.writeLock();
    protected final Object lock = new Object();
    protected final ConcurrentMap<String, InstanceInfo.InstanceStatus> overriddenInstanceStatusMap = CacheBuilder
            .newBuilder().initialCapacity(500)
            .expireAfterAccess(1, TimeUnit.HOURS)
            .<String, InstanceInfo.InstanceStatus>build().asMap();
    // delta queue
    private ConcurrentLinkedQueue<RecentlyChangedItem> recentlyChangedQueue = new ConcurrentLinkedQueue<RecentlyChangedItem>();
    protected volatile ResponseCache responseCache;
    CachedPantheonServerConfig serverConfig;
    private Timer deltaRetentionTimer = new Timer("Pantheon-DeltaRetentionTimer", true);


    private InstanceRegistryImpl() {
        this.serverConfig=CachedPantheonServerConfig.getInstance();
        responseCache = new ResponseCacheImpl(serverConfig, this);
        this.deltaRetentionTimer.schedule(getDeltaRetentionTask(),
                serverConfig.getDeltaRetentionTimerIntervalInMs(),
                serverConfig.getDeltaRetentionTimerIntervalInMs());
    }

    private static class Singleton {
        static InstanceRegistryImpl instance = new InstanceRegistryImpl();
    }

    public static InstanceRegistryImpl getInstance() {
        return InstanceRegistryImpl.Singleton.instance;
    }

    public void register(final InstanceInfo info) {
        int leaseDuration = Lease.DEFAULT_DURATION_IN_SECS;
        if (info.getLeaseInfo() != null && info.getLeaseInfo().getDurationInSecs() > 0) {
            leaseDuration = info.getLeaseInfo().getDurationInSecs();
        }
        this.register(info, leaseDuration);
    }

    /**
     * Registers a new instance with a given duration.
     *
     * @see LeaseManager#register(java.lang.Object, int)
     */
    public void register(InstanceInfo registrant, int leaseDuration) {
        try {
            read.lock();
            Map<String, Lease<InstanceInfo>> gMap = registry.get(registrant.getAppName());
            if (gMap == null) {
                //build new map
                final ConcurrentHashMap<String, Lease<InstanceInfo>> gNewMap = new ConcurrentHashMap<String, Lease<InstanceInfo>>();
                gMap = registry.putIfAbsent(registrant.getAppName(), gNewMap);
                if (gMap == null) {
                    gMap = gNewMap;
                }
            }
            Lease<InstanceInfo> existingLease = gMap.get(registrant.getId());
            // Retain the last dirty timestamp without overwriting it, if there is already a lease
            if (existingLease != null && (existingLease.getHolder() != null)) {
                Long existingLastDirtyTimestamp = existingLease.getHolder().getLastDirtyTimestamp();
                Long registrationLastDirtyTimestamp = registrant.getLastDirtyTimestamp();
                logger.debug("Existing lease found (existing={}, provided={}", existingLastDirtyTimestamp, registrationLastDirtyTimestamp);

                // this is a > instead of a >= because if the timestamps are equal, we still take the remote transmitted
                // InstanceInfo instead of the server local copy.
                if (existingLastDirtyTimestamp > registrationLastDirtyTimestamp) {
                    logger.warn("There is an existing lease and the existing lease's dirty timestamp {} is greater" +
                            " than the one that is being registered {}", existingLastDirtyTimestamp, registrationLastDirtyTimestamp);
                    logger.warn("Using the existing instanceInfo instead of the new instanceInfo as the registrant");
                    registrant = existingLease.getHolder();
                }
            } else {
                logger.debug("No previous lease information found; it is new registration");
            }
            Lease<InstanceInfo> lease = new Lease<InstanceInfo>(registrant, leaseDuration);
            if (existingLease != null) {
                lease.setServiceUpTimestamp(existingLease.getServiceUpTimestamp());
            }
            gMap.put(registrant.getId(), lease);

//            // This is where the initial state transfer of overridden status happens
            if (!InstanceInfo.InstanceStatus.UNKNOWN.equals(registrant.getOverriddenStatus())) {
                logger.debug("Found overridden status {} for instance {}. Checking to see if needs to be add to the "
                        + "overrides", registrant.getOverriddenStatus(), registrant.getId());
                if (!overriddenInstanceStatusMap.containsKey(registrant.getId())) {
                    logger.info("Not found overridden id {} and hence adding it", registrant.getId());
                    overriddenInstanceStatusMap.put(registrant.getId(), registrant.getOverriddenStatus());
                }
            }
            InstanceInfo.InstanceStatus overriddenStatusFromMap = overriddenInstanceStatusMap.get(registrant.getId());
            if (overriddenStatusFromMap != null) {
                logger.info("Storing overridden status {} from map", overriddenStatusFromMap);
                registrant.setOverriddenStatus(overriddenStatusFromMap);
            }

            // Set the status based on the overridden status rules
            InstanceInfo.InstanceStatus overriddenInstanceStatus = getOverriddenInstanceStatus(registrant, existingLease);
            registrant.setStatusWithoutDirty(overriddenInstanceStatus);

            // If the lease is registered with UP status, set lease service up timestamp
            if (InstanceInfo.InstanceStatus.UP.equals(registrant.getStatus())) {
                lease.serviceUp();
            }
            registrant.setActionType(InstanceInfo.ActionType.ADDED);
            recentlyChangedQueue.add(new RecentlyChangedItem(lease));
            registrant.setLastUpdatedTimestamp();
            responseCache.invalidate(registrant.getAppName());

            logger.info("Registered instance {} with status {}",
                    registrant.getAppName() + "/" + registrant.getId(), registrant.getStatus());
        } finally {
            read.unlock();
        }
    }

    public InstanceInfo.InstanceStatus getOverriddenInstanceStatus(InstanceInfo r,
                                                                      Lease<InstanceInfo> existingLease) {
        InstanceStatusOverrideRule rule =  new FirstMatchWinsCompositeRule(new DownOrStartingRule(),
                new OverrideExistsRule(overriddenInstanceStatusMap), new LeaseExistsRule());
        logger.debug("Processing override status using rule: {}", rule);
        return rule.apply(r, existingLease).status();
    }


    /**
     * Get the registry information about all {@link Applications}.
     */
    public Applications getApplications() {

        Applications apps = new Applications();
        for (Map.Entry<String, Map<String, Lease<InstanceInfo>>> entry : registry.entrySet()) {
            Application app = null;

            if (entry.getValue() != null) {
                for (Map.Entry<String, Lease<InstanceInfo>> stringLeaseEntry : entry.getValue().entrySet()) {
                    Lease<InstanceInfo> lease = stringLeaseEntry.getValue();
                    if (app == null) {
                        app = new Application(lease.getHolder().getAppName());
                    }
                    app.addInstance(decorateInstanceInfo(lease));
                }
            }
            if (app != null) {
                apps.addApplication(app);
            }
        }
        apps.setAppsHashCode(apps.getReconcileHashCode());
        return apps;
    }

    private InstanceInfo decorateInstanceInfo(Lease<InstanceInfo> lease) {
        InstanceInfo info = lease.getHolder();

        // client app settings
        int renewalInterval = LeaseInfo.DEFAULT_LEASE_RENEWAL_INTERVAL;
        int leaseDuration = LeaseInfo.DEFAULT_LEASE_DURATION;


        if (info.getLeaseInfo() != null) {
            renewalInterval = info.getLeaseInfo().getRenewalIntervalInSecs();
            leaseDuration = info.getLeaseInfo().getDurationInSecs();
        }

        info.setLeaseInfo(LeaseInfo.Builder.newBuilder()
                .setRegistrationTimestamp(lease.getRegistrationTimestamp())
                .setRenewalTimestamp(lease.getLastRenewalTimestamp())
                .setServiceUpTimestamp(lease.getServiceUpTimestamp())
                .setRenewalIntervalInSecs(renewalInterval)
                .setDurationInSecs(leaseDuration)
                .setEvictionTimestamp(lease.getEvictionTimestamp()).build());

        return info;
    }

    /**
     * Get the registry information about the delta changes. The deltas are
     * cached for a window specified by
     * {@link PantheonServerConfig#getRetentionTimeInMSInDeltaQueue()}. Subsequent
     * requests for delta information may return the same information and client
     * must make sure this does not adversely affect them.
     *
     * @return all application deltas.
     */
    public Applications getApplicationDeltas() {

        Applications apps = new Applications();
        Map<String, Application> applicationInstancesMap = new HashMap<String, Application>();
        try {
            write.lock();
            Iterator<RecentlyChangedItem> iter = this.recentlyChangedQueue.iterator();
            logger.debug("The number of elements in the delta queue is :" + this.recentlyChangedQueue.size());
            while (iter.hasNext()) {
                Lease<InstanceInfo> lease = iter.next().getLeaseInfo();
                InstanceInfo instanceInfo = lease.getHolder();
                Object[] args = {instanceInfo.getId(),
                        instanceInfo.getStatus().name(),
                        instanceInfo.getActionType().name()};
                logger.debug("The instance id %s is found with status %s and actiontype %s", args);
                Application app = applicationInstancesMap.get(instanceInfo.getAppName());
                if (app == null) {
                    app = new Application(instanceInfo.getAppName());
                    applicationInstancesMap.put(instanceInfo.getAppName(), app);
                    apps.addApplication(app);
                }
                app.addInstance(decorateInstanceInfo(lease));
            }


            Applications allApps = getApplications();
            apps.setAppsHashCode(allApps.getReconcileHashCode());
            return apps;
        } finally {
            write.unlock();
        }
    }

    public InstanceInfo getInstanceByAppAndId(String appName, String id) {
        Map<String, Lease<InstanceInfo>> leaseMap = registry.get(appName);
        Lease<InstanceInfo> lease = null;
        if (leaseMap != null) {
            lease = leaseMap.get(id);
        }
        if (lease != null
                && (!lease.isExpired())) {
            return decorateInstanceInfo(lease);
        }
        return null;
    }

    /**
     * Marks the given instance of the given app name as renewed, and also marks whether it originated from
     * replication.
     *
     * @see LeaseManager#renew(java.lang.String, java.lang.String)
     */
    public boolean renew(String appName, String id) {
        Map<String, Lease<InstanceInfo>> gMap = registry.get(appName);
        Lease<InstanceInfo> leaseToRenew = null;
        if (gMap != null) {
            leaseToRenew = gMap.get(id);
        }
        if (leaseToRenew == null) {
            logger.warn("Registry: lease doesn't exist, registering resource: {} - {}", appName, id);
            return false;
        } else {
            InstanceInfo instanceInfo = leaseToRenew.getHolder();
            if (instanceInfo != null) {

                InstanceInfo.InstanceStatus overriddenInstanceStatus = this.getOverriddenInstanceStatus(
                        instanceInfo, leaseToRenew);
                if (overriddenInstanceStatus == InstanceInfo.InstanceStatus.UNKNOWN) {
                    logger.info("Instance status UNKNOWN possibly due to deleted override for instance {}"
                            + "; re-register required", instanceInfo.getId());
                    return false;
                }
                if (!instanceInfo.getStatus().equals(overriddenInstanceStatus)) {
                    Object[] args = {
                            instanceInfo.getStatus().name(),
                            instanceInfo.getOverriddenStatus().name(),
                            instanceInfo.getId()
                    };
                    logger.info(
                            "The instance status {} is different from overridden instance status {} for instance {}. "
                                    + "Hence setting the status to overridden status", args);
                    instanceInfo.setStatusWithoutDirty(overriddenInstanceStatus);
                }
            }
            leaseToRenew.renew();
            return true;
        }
    }

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
    public boolean statusUpdate(String appName, String id,
                                InstanceInfo.InstanceStatus newStatus, String lastDirtyTimestamp) {
        try {
            read.lock();
            Map<String, Lease<InstanceInfo>> gMap = registry.get(appName);
            Lease<InstanceInfo> lease = null;
            if (gMap != null) {
                lease = gMap.get(id);
            }
            if (lease == null) {
                return false;
            } else {
                lease.renew();
                InstanceInfo info = lease.getHolder();
                // Lease is always created with its instance info object.
                // This log statement is provided as a safeguard, in case this invariant is violated.
                if (info == null) {
                    logger.error("Found Lease without a holder for instance id {}", id);
                }
                if ((info != null) && !(info.getStatus().equals(newStatus))) {
                    // Mark service as UP if needed
                    if (InstanceInfo.InstanceStatus.UP.equals(newStatus)) {
                        lease.serviceUp();
                    }
                    // This is NAC overriden status
                    overriddenInstanceStatusMap.put(id, newStatus);
                    // Set it for transfer of overridden status to replica on
                    // replica start up
                    info.setOverriddenStatus(newStatus);
                    long replicaDirtyTimestamp = 0;
                    info.setStatusWithoutDirty(newStatus);
                    if (lastDirtyTimestamp != null) {
                        replicaDirtyTimestamp = Long.valueOf(lastDirtyTimestamp);
                    }
                    // If the replication's dirty timestamp is more than the existing one, just update
                    // it to the replica's.
                    if (replicaDirtyTimestamp > info.getLastDirtyTimestamp()) {
                        info.setLastDirtyTimestamp(replicaDirtyTimestamp);
                    }
                    info.setActionType(InstanceInfo.ActionType.MODIFIED);
                    recentlyChangedQueue.add(new RecentlyChangedItem(lease));
                    info.setLastUpdatedTimestamp();
                    responseCache.invalidate(appName);

                }
                return true;
            }
        } finally {
            read.unlock();
        }
    }

    public void storeOverriddenStatusIfRequired(String appName, String id, InstanceInfo.InstanceStatus overriddenStatus) {
        InstanceInfo.InstanceStatus instanceStatus = overriddenInstanceStatusMap.get(id);
        if ((instanceStatus == null) || (!overriddenStatus.equals(instanceStatus))) {
            // We might not have the overridden status if the server got
            // restarted -this will help us maintain the overridden state
            // from the replica
            logger.info("Adding overridden status for instance id {} and the value is {}",
                    id, overriddenStatus.name());
            overriddenInstanceStatusMap.put(id, overriddenStatus);
            InstanceInfo instanceInfo = this.getInstanceByAppAndId(appName, id);
            instanceInfo.setOverriddenStatus(overriddenStatus);
            logger.info("Set the overridden status for instance (appname:{}} and the value is {} ",
                    appName + ",id:" + id, overriddenStatus.name());
        }
    }

    /**
     * Removes status override for a give instance.
     *
     * @param appName            the application name of the instance.
     * @param id                 the unique identifier of the instance.
     * @param newStatus          the new {@link InstanceInfo.InstanceStatus}.
     * @param lastDirtyTimestamp last timestamp when this instance information was updated.
     * @return true if the status was successfully updated, false otherwise.
     */
    public boolean deleteStatusOverride(String appName, String id,
                                        InstanceInfo.InstanceStatus newStatus,
                                        String lastDirtyTimestamp) {
        try {
            read.lock();
            Map<String, Lease<InstanceInfo>> gMap = registry.get(appName);
            Lease<InstanceInfo> lease = null;
            if (gMap != null) {
                lease = gMap.get(id);
            }
            if (lease == null) {
                return false;
            } else {
                lease.renew();
                InstanceInfo info = lease.getHolder();

                // Lease is always created with its instance info object.
                // This log statement is provided as a safeguard, in case this invariant is violated.
                if (info == null) {
                    logger.error("Found Lease without a holder for instance id {}", id);
                }

                InstanceInfo.InstanceStatus currentOverride = overriddenInstanceStatusMap.remove(id);
                if (currentOverride != null && info != null) {
                    info.setOverriddenStatus(InstanceInfo.InstanceStatus.UNKNOWN);
                    info.setStatusWithoutDirty(newStatus);
                    long replicaDirtyTimestamp = 0;
                    if (lastDirtyTimestamp != null) {
                        replicaDirtyTimestamp = Long.valueOf(lastDirtyTimestamp);
                    }
                    // If the replication's dirty timestamp is more than the existing one, just update
                    // it to the replica's.
                    if (replicaDirtyTimestamp > info.getLastDirtyTimestamp()) {
                        info.setLastDirtyTimestamp(replicaDirtyTimestamp);
                    }
                    info.setActionType(InstanceInfo.ActionType.MODIFIED);
                    recentlyChangedQueue.add(new RecentlyChangedItem(lease));
                    info.setLastUpdatedTimestamp();
                    responseCache.invalidate(appName);
                }
                return true;
            }
        } finally {
            read.unlock();
        }
    }

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
    public boolean cancel(String appName, String id) {
        try {
            read.lock();
            Map<String, Lease<InstanceInfo>> gMap = registry.get(appName);
            Lease<InstanceInfo> leaseToCancel = null;
            if (gMap != null) {
                leaseToCancel = gMap.remove(id);
            }
            InstanceInfo.InstanceStatus instanceStatus = overriddenInstanceStatusMap.remove(id);
            if (instanceStatus != null) {
                logger.debug("Removed instance id {} from the overridden map which has value {}", id, instanceStatus.name());
            }
            if (leaseToCancel == null) {
                logger.warn("Registry: cancel failed because Lease is not registered for: {}/{}", appName, id);
                return false;
            } else {
                leaseToCancel.cancel();
                InstanceInfo instanceInfo = leaseToCancel.getHolder();
                if (instanceInfo != null) {
                    instanceInfo.setActionType(InstanceInfo.ActionType.DELETED);
                    recentlyChangedQueue.add(new RecentlyChangedItem(leaseToCancel));
                    instanceInfo.setLastUpdatedTimestamp();
                }
                responseCache.invalidate(appName);
                logger.info("Cancelled instance {}/{} ", appName, id);
                return true;
            }
        } finally {
            read.unlock();
        }
    }

    public Application getApplication(String appName) {
        Application app = null;

        Map<String, Lease<InstanceInfo>> leaseMap = registry.get(appName);

        if (leaseMap != null && leaseMap.size() > 0) {
            for (Map.Entry<String, Lease<InstanceInfo>> entry : leaseMap.entrySet()) {
                if (app == null) {
                    app = new Application(appName);
                }
                app.addInstance(decorateInstanceInfo(entry.getValue()));
            }
        }
        return app;
    }

    private static final class RecentlyChangedItem {
        private long lastUpdateTime;
        private Lease<InstanceInfo> leaseInfo;

        public RecentlyChangedItem(Lease<InstanceInfo> lease) {
            this.leaseInfo = lease;
            lastUpdateTime = System.currentTimeMillis();
        }

        public long getLastUpdateTime() {
            return this.lastUpdateTime;
        }

        public Lease<InstanceInfo> getLeaseInfo() {
            return this.leaseInfo;
        }
    }

    public ResponseCache getResponseCache() {
        return responseCache;
    }

    /**
     * remove data from recentlyChangedQueue
     */
    private TimerTask getDeltaRetentionTask() {
        return new TimerTask() {

            @Override
            public void run() {
                Iterator<RecentlyChangedItem> it = recentlyChangedQueue.iterator();
                while (it.hasNext()) {
                    if (it.next().getLastUpdateTime() <
                            //default 180s
                            System.currentTimeMillis() - serverConfig.getRetentionTimeInMSInDeltaQueue()) {
                        it.remove();
                    } else {
                        break;
                    }
                }
            }
        };
    }
}
