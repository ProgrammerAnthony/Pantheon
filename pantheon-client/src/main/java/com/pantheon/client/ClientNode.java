package com.pantheon.client;

import com.pantheon.client.appinfo.Application;
import com.pantheon.client.appinfo.Applications;
import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.client.config.DefaultInstanceConfig;
import com.pantheon.common.ThreadFactoryImpl;
import com.pantheon.remoting.exception.RemotingCommandException;
import com.pantheon.remoting.exception.RemotingConnectException;
import com.pantheon.remoting.exception.RemotingSendRequestException;
import com.pantheon.remoting.exception.RemotingTimeoutException;
import com.pantheon.remoting.netty.NettyClientConfig;
import io.netty.bootstrap.ServerBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Anthony
 * @create 2021/11/19
 * @desc
 **/
public class ClientNode {
    private NettyClientConfig nettyClientConfig;
    private DefaultInstanceConfig instanceConfig;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "InstanceControllerScheduledThread"));
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);
    private ClientAPIImpl clientAPI;
    private Server server;
    private final Lock lockHeartbeat = new ReentrantLock();
    private final String clientId;
    private final AtomicLong fetchRegistryGeneration;
    private final AtomicReference<Applications> localRegionApps = new AtomicReference<Applications>();
    private volatile int registrySize = 0;
    private volatile long lastSuccessfulRegistryFetchTimestamp = -1;
    private volatile long lastSuccessfulHeartbeatTimestamp = -1;
    private final Lock fetchRegistryUpdateLock = new ReentrantLock();
    private final InstanceInfo instanceInfo;
    private volatile InstanceInfo.InstanceStatus lastRemoteInstanceStatus = InstanceInfo.InstanceStatus.UNKNOWN;


//todo add eventbus support
// private final CopyOnWriteArraySet<PantheonEventListener> eventListeners = new CopyOnWriteArraySet<>();


    public ClientNode(NettyClientConfig nettyClientConfig, DefaultInstanceConfig instanceConfig, String clientId) {
        this.nettyClientConfig = nettyClientConfig;
        this.instanceConfig = instanceConfig;
        this.clientId = clientId;
        fetchRegistryGeneration = new AtomicLong(0);
        localRegionApps.set(new Applications());
        instanceInfo = new InstanceInfo();//todo build myself instance info
        clientAPI = new ClientAPIImpl(nettyClientConfig, instanceConfig, new ClientRemotingProcessor(), null);
    }


    public boolean start() {
        clientAPI.start();

        //choose a controller candidate from local config
        String controllerCandidate = this.clientAPI.chooseControllerCandidate();
        Integer nodeId = null;
        try {
            nodeId = this.clientAPI.fetchServerNodeId(controllerCandidate, 10000);
            logger.info("fetchServerNodeId successful load nodeId: " + nodeId);
            Map<String, List<String>> integerListMap = this.clientAPI.fetchSlotsAllocation(controllerCandidate, 10000);
            logger.info("fetchSlotsAllocation successful load map: " + integerListMap);

            Map<String, Server> serverMap = this.clientAPI.fetchServerAddresses(controllerCandidate, 1000);
            logger.info("fetchServerAddresses successful load map: " + serverMap);

            String serviceName = instanceConfig.getServiceName();
            server = this.clientAPI.routeServer(serviceName);

            this.startScheduledTask();
        } catch (RemotingConnectException e) {
            e.printStackTrace();
        } catch (RemotingSendRequestException e) {
            e.printStackTrace();
        } catch (RemotingTimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (RemotingCommandException e) {
            e.printStackTrace();
        }
        return true;
    }

    private void startScheduledTask() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                sendHeartBeatToServer(server);
            }
        }, 1000, instanceConfig.getLeaseRenewalIntervalInSeconds() * 1000, TimeUnit.MILLISECONDS);
        //heartbeat
    }

    private void sendHeartBeatToServer(Server server) {
        if (this.lockHeartbeat.tryLock()) {
            try {
                boolean successResult = this.clientAPI.sendHeartBeatToServer(server, clientId, 3000L);
                if (successResult) {
                    logger.info("heartbeat success!!!");
                }
            } catch (final Exception e) {
                logger.error("sendHeartBeatToServer exception", e);
            } finally {
                this.lockHeartbeat.unlock();
            }
        } else {
            logger.warn("lock heartBeat, but failed. [{}]", instanceConfig.getServiceName());
        }
    }

    public void shutdown() {
        this.clientAPI.shutdown();
    }

    public void sendRegister() {
        if (instanceConfig.shouldFetchRegistry()) {
            boolean fetchRegistryResult = fetchRegistry(false);
            if (fetchRegistryResult) {
                logger.info("service registry success!!!");
            }
        }

//            try {
//            boolean registryResult = this.clientAPI.serviceRegistry(server.getRemoteSocketAddress(), 1000);
//            if (registryResult) {
//                logger.info("service registry success!!!");
//            }
//        } catch (RemotingConnectException e) {
//            e.printStackTrace();
//        } catch (RemotingSendRequestException e) {
//            e.printStackTrace();
//        } catch (RemotingTimeoutException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    /**
     * The task that fetches the registry information at specified intervals.
     */
    class CacheRefreshThread implements Runnable {
        public void run() {
            refreshRegistry();
        }
    }

    void refreshRegistry() {
        try {
            boolean success = fetchRegistry(true);
            if (success) {
                registrySize = localRegionApps.get().size();
                lastSuccessfulRegistryFetchTimestamp = System.currentTimeMillis();
            }

        } catch (Throwable e) {
            logger.error("Cannot fetch registry from server", e);
        }
    }


    /**
     * Fetches the registry information.
     *
     * <p>
     * This method tries to get only deltas after the first fetch unless there
     * is an issue in reconciling pantheon server and client registry information.
     * </p>
     *
     * @param forceFullRegistryFetch Forces a full registry fetch.
     * @return true if the registry was fetched
     */
    private boolean fetchRegistry(boolean forceFullRegistryFetch) {

        try {
            // If the delta is disabled or if it is the first time, get all
            // applications
            Applications applications = getApplications();

            if (forceFullRegistryFetch
                    || (applications == null)
                    || (applications.getRegisteredApplications().size() == 0)) {
                logger.info("Force full registry fetch : {}", forceFullRegistryFetch);
                logger.info("Application is null : {}", (applications == null));
                logger.info("Registered Applications size is zero : {}",
                        (applications.getRegisteredApplications().size() == 0));
                getAndStoreFullRegistry();
            } else {
                getAndUpdateDelta(applications);
            }
            applications.setAppsHashCode(applications.getReconcileHashCode());
            logTotalInstances();
        } catch (Throwable e) {
            logger.error("ClientNode was unable to refresh its cache! status = " + e.getMessage(), e);
            return false;
        }

        // Notify about cache refresh before updating the instance remote status
        onCacheRefreshed();

        // Update remote status based on refreshed data held in the cache
        updateInstanceRemoteStatus();

        // registry was fetched successfully, so return true
        return true;
    }

    /**
     * Logs the total number of non-filtered instances stored locally.
     */
    private void logTotalInstances() {
        if (logger.isDebugEnabled()) {
            int totInstances = 0;
            for (Application application : getApplications().getRegisteredApplications()) {
                totInstances += application.getInstancesAsIsFromPantheon().size();
            }
            logger.debug("The total number of all instances in the client now is {}", totInstances);
        }
    }

    /**
     * Invoked every time the local registry cache is refreshed (whether changes have
     * been detected or not).
     * <p>
     * Subclasses may override this method to implement custom behavior if needed.
     */
    protected void onCacheRefreshed() {
//        fireEvent(new CacheRefreshedEvent());
    }


    private synchronized void updateInstanceRemoteStatus() {
        // Determine this instance's status for this app and set to UNKNOWN if not found
        InstanceInfo.InstanceStatus currentRemoteInstanceStatus = null;
        if (instanceInfo.getAppName() != null) {
            Application app = getApplication(instanceInfo.getAppName());
            if (app != null) {
                InstanceInfo remoteInstanceInfo = app.getByInstanceId(instanceInfo.getId());
                if (remoteInstanceInfo != null) {
                    currentRemoteInstanceStatus = remoteInstanceInfo.getStatus();
                }
            }
        }
        if (currentRemoteInstanceStatus == null) {
            currentRemoteInstanceStatus = InstanceInfo.InstanceStatus.UNKNOWN;
        }

        // Notify if status changed
        if (lastRemoteInstanceStatus != currentRemoteInstanceStatus) {
            onRemoteStatusChanged(lastRemoteInstanceStatus, currentRemoteInstanceStatus);
            lastRemoteInstanceStatus = currentRemoteInstanceStatus;
        }
    }

    public Applications getApplications() {
        return localRegionApps.get();
    }

    public Application getApplication(String appName) {
        return getApplications().getRegisteredApplications(appName);
    }

    /**
     * Gets the full registry information from the pantheon server and stores it locally.
     * When applying the full registry, the following flow is observed:
     * <p>
     * if (update generation have not advanced (due to another thread))
     * atomically set the registry to the new registry
     * fi
     *
     * @return the full registry information.
     * @throws Throwable on error.
     */
    private void getAndStoreFullRegistry() throws Throwable {
        long currentUpdateGeneration = fetchRegistryGeneration.get();

        logger.info("Getting all instance registry info from the pantheon server");

        Applications apps = this.clientAPI.getApplications(server, 3000L);

        if (apps == null) {
            logger.error("The application is null for some reason. Not storing this information");
            //AtomicLong fetchRegistryGeneration to  fix multi-thread data conflict
        } else if (fetchRegistryGeneration.compareAndSet(currentUpdateGeneration, currentUpdateGeneration + 1)) {
            apps.shuffleInstances(instanceConfig.shouldFilterOnlyUpInstances());
            localRegionApps.set(apps);

            logger.debug("Got full registry with apps hashcode {}", apps.getAppsHashCode());
        } else {
            logger.warn("Not updating applications as another thread is updating it already");
        }
    }


    /**
     * 获取更新增量信息，通过读写锁优化性能
     * Get the delta registry information from the pantheon server and update it locally.
     * When applying the delta, the following flow is observed:
     * <p>
     * if (update generation have not advanced (due to another thread))
     * atomically try to: update application with the delta and get reconcileHashCode
     * abort entire processing otherwise
     * do reconciliation if reconcileHashCode clash
     * fi
     *
     * @return the client response
     * @throws Throwable on error
     */
    private void getAndUpdateDelta(Applications applications) throws Throwable {
        long currentUpdateGeneration = fetchRegistryGeneration.get();

        Applications delta = null;

        Applications apps = this.clientAPI.getDelta(server, 3000L);


        if (delta == null) {
            logger.warn("The server does not allow the delta revision to be applied because it is not safe. "
                    + "Hence got the full registry.");
            getAndStoreFullRegistry();
        } else if (fetchRegistryGeneration.compareAndSet(currentUpdateGeneration, currentUpdateGeneration + 1)) {
            logger.debug("Got delta update with apps hashcode {}", delta.getAppsHashCode());
            String reconcileHashCode = "";
            if (fetchRegistryUpdateLock.tryLock()) {
                try {
                    updateDelta(delta);
                    reconcileHashCode = getReconcileHashCode(applications);
                } finally {
                    fetchRegistryUpdateLock.unlock();
                }
            } else {
                logger.warn("Cannot acquire update lock, aborting getAndUpdateDelta");
            }
            // There is a diff in number of instances for some reason
            if (!reconcileHashCode.equals(delta.getAppsHashCode())) {
                reconcileAndLogDifference(delta, reconcileHashCode);  // this makes a remoteCall
            }
        } else {
            logger.warn("Not updating application delta as another thread is updating it already");
            logger.debug("Ignoring delta update with apps hashcode {}, as another thread is updating it already", delta.getAppsHashCode());
        }
    }


    private String getReconcileHashCode(Applications applications) {
        return null;
//        TreeMap<String, AtomicInteger> instanceCountMap = new TreeMap<String, AtomicInteger>();
//        if (isFetchingRemoteRegionRegistries()) {
//            for (Applications remoteApp : remoteRegionVsApps.values()) {
//                remoteApp.populateInstanceCountMap(instanceCountMap);
//            }
//        }
//        applications.populateInstanceCountMap(instanceCountMap);
//        return Applications.getReconcileHashCode(instanceCountMap);
    }

    /**
     * 发现数量不一致的情况，通过重新拉去注册表更新
     * Reconcile the pantheon server and client registry information and logs the differences if any.
     * When reconciling, the following flow is observed:
     * <p>
     * make a remote call to the server for the full registry
     * calculate and log differences
     * if (update generation have not advanced (due to another thread))
     * atomically set the registry to the new registry
     * fi
     *
     * @param delta             the last delta registry information received from the pantheon
     *                          server.
     * @param reconcileHashCode the hashcode generated by the server for reconciliation.
     * @return ClientResponse the HTTP response object.
     * @throws Throwable on any error.
     */
    private void reconcileAndLogDifference(Applications delta, String reconcileHashCode) throws Throwable {
        logger.debug("The Reconcile hashcodes do not match, client : {}, server : {}. Getting the full registry",
                reconcileHashCode, delta.getAppsHashCode());


        long currentUpdateGeneration = fetchRegistryGeneration.get();

        Applications serverApps = getApplications();

        if (serverApps == null) {
            logger.warn("Cannot fetch full registry from the server; reconciliation failure");
            return;
        }

        if (logger.isDebugEnabled()) {
            try {
                Map<String, List<String>> reconcileDiffMap = getApplications().getReconcileMapDiff(serverApps);
                StringBuilder reconcileBuilder = new StringBuilder("");
                for (Map.Entry<String, List<String>> mapEntry : reconcileDiffMap.entrySet()) {
                    reconcileBuilder.append(mapEntry.getKey()).append(": ");
                    for (String displayString : mapEntry.getValue()) {
                        reconcileBuilder.append(displayString);
                    }
                    reconcileBuilder.append('\n');
                }
                String reconcileString = reconcileBuilder.toString();
                logger.debug("The reconcile string is {}", reconcileString);
            } catch (Throwable e) {
                logger.error("Could not calculate reconcile string ", e);
            }
        }

        if (fetchRegistryGeneration.compareAndSet(currentUpdateGeneration, currentUpdateGeneration + 1)) {
            serverApps.shuffleInstances(true);
            localRegionApps.set(serverApps);
            logger.debug(
                    "The Reconcile hashcodes after complete sync up, client : {}, server : {}.",
                    getApplications().getReconcileHashCode(),
                    delta.getAppsHashCode());
        } else {
            logger.warn("Not setting the applications map as another thread has advanced the update generation");
        }
    }


    /**
     * Updates the delta information fetches from the pantheon server into the
     * local cache.
     *
     * @param delta the delta information received from pantheon server in the last
     *              poll cycle.
     */
    private void updateDelta(Applications delta) {
        int deltaCount = 0;
        for (Application app : delta.getRegisteredApplications()) {
            for (InstanceInfo instance : app.getInstances()) {
                Applications applications = getApplications();

                ++deltaCount;
                if (InstanceInfo.ActionType.ADDED.equals(instance.getActionType())) {
                    Application existingApp = applications.getRegisteredApplications(instance.getAppName());
                    if (existingApp == null) {
                        applications.addApplication(app);
                    }
                    logger.debug("Added instance {} to the existing apps ", instance.getId());
                    applications.getRegisteredApplications(instance.getAppName()).addInstance(instance);
                } else if (InstanceInfo.ActionType.MODIFIED.equals(instance.getActionType())) {
                    Application existingApp = applications.getRegisteredApplications(instance.getAppName());
                    if (existingApp == null) {
                        applications.addApplication(app);
                    }
                    logger.debug("Modified instance {} to the existing apps ", instance.getId());

                    applications.getRegisteredApplications(instance.getAppName()).addInstance(instance);

                } else if (InstanceInfo.ActionType.DELETED.equals(instance.getActionType())) {
                    Application existingApp = applications.getRegisteredApplications(instance.getAppName());
                    if (existingApp == null) {
                        applications.addApplication(app);
                    }
                    logger.debug("Deleted instance {} to the existing apps ", instance.getId());
                    applications.getRegisteredApplications(instance.getAppName()).removeInstance(instance);
                }
            }
        }
        logger.debug("The total number of instances fetched by the delta processor : {}", deltaCount);

        getApplications().shuffleInstances(instanceConfig.shouldFilterOnlyUpInstances());

    }

    /**
     * Invoked when the remote status of this client has changed.
     * Subclasses may override this method to implement custom behavior if needed.
     *
     * @param oldStatus the previous remote {@link InstanceInfo.InstanceStatus}
     * @param newStatus the new remote {@link InstanceInfo.InstanceStatus}
     */
    protected void onRemoteStatusChanged(InstanceInfo.InstanceStatus oldStatus, InstanceInfo.InstanceStatus newStatus) {
//        fireEvent(new StatusChangeEvent(oldStatus, newStatus));
    }


    public void registerEventListener(PantheonEventListener eventListener) {
//        this.eventListeners.add(eventListener);
    }

}
