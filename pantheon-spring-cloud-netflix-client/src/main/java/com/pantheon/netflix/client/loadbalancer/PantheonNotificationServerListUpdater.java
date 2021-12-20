package com.pantheon.netflix.client.loadbalancer;

import com.netflix.config.DynamicIntProperty;
import com.netflix.loadbalancer.ServerListUpdater;

import com.pantheon.client.CacheRefreshedEvent;
import com.pantheon.client.DiscoveryClientNode;
import com.pantheon.client.PantheonEvent;
import com.pantheon.client.PantheonEventListener;
import com.pantheon.client.discovery.DiscoveryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Anthony
 * @create 2021/12/20
 * @desc
 **/
public class PantheonNotificationServerListUpdater implements ServerListUpdater {
    private static final Logger logger = LoggerFactory.getLogger(PantheonNotificationServerListUpdater.class);
    final AtomicBoolean updateQueued;
    private final AtomicBoolean isActive;
    private final AtomicLong lastUpdated;
    private final Provider<DiscoveryClient> pantheonClientProvider;
    private final ExecutorService refreshExecutor;
    private volatile PantheonEventListener updateListener;
    private volatile DiscoveryClientNode pantheonClient;

    public static ExecutorService getDefaultRefreshExecutor() {
        return PantheonNotificationServerListUpdater.LazyHolder.SINGLETON.defaultServerListUpdateExecutor;
    }

    public PantheonNotificationServerListUpdater() {
        this(new LegacyEurekaClientProvider());
    }

    public PantheonNotificationServerListUpdater(Provider<DiscoveryClient> eurekaClientProvider) {
        this(pantheonClientProvider, getDefaultRefreshExecutor());
    }

    public PantheonNotificationServerListUpdater(Provider<DiscoveryClient> pantheonClientProvider, ExecutorService refreshExecutor) {
        this.updateQueued = new AtomicBoolean(false);
        this.isActive = new AtomicBoolean(false);
        this.lastUpdated = new AtomicLong(System.currentTimeMillis());
        this.pantheonClientProvider = pantheonClientProvider;
        this.refreshExecutor = refreshExecutor;
    }

    public synchronized void start(final UpdateAction updateAction) {
        if (this.isActive.compareAndSet(false, true)) {
            this.updateListener = new PantheonEventListener() {
                public void onEvent(PantheonEvent event) {
                    if (event instanceof CacheRefreshedEvent) {
                        if (!PantheonNotificationServerListUpdater.this.updateQueued.compareAndSet(false, true)) {
                            PantheonNotificationServerListUpdater.logger.info("an update action is already queued, returning as no-op");
                            return;
                        }

                        if (!PantheonNotificationServerListUpdater.this.refreshExecutor.isShutdown()) {
                            try {
                                PantheonNotificationServerListUpdater.this.refreshExecutor.submit(new Runnable() {
                                    public void run() {
                                        try {
                                            updateAction.doUpdate();
                                            PantheonNotificationServerListUpdater.this.lastUpdated.set(System.currentTimeMillis());
                                        } catch (Exception var5) {
                                            PantheonNotificationServerListUpdater.logger.warn("Failed to update serverList", var5);
                                        } finally {
                                            PantheonNotificationServerListUpdater.this.updateQueued.set(false);
                                        }

                                    }
                                });
                            } catch (Exception var3) {
                                PantheonNotificationServerListUpdater.logger.warn("Error submitting update task to executor, skipping one round of updates", var3);
                                PantheonNotificationServerListUpdater.this.updateQueued.set(false);
                            }
                        } else {
                            PantheonNotificationServerListUpdater.logger.debug("stopping PantheonNotificationServerListUpdater, as refreshExecutor has been shut down");
                            PantheonNotificationServerListUpdater.this.stop();
                        }
                    }

                }
            };
            if (this.pantheonClient == null) {
                this.pantheonClient = (DiscoveryClientNode) this.pantheonClientProvider.get();
            }

            if (this.pantheonClient == null) {
                logger.error("Failed to register an updateListener to pantheon client, pantheon client is null");
                throw new IllegalStateException("Failed to start the updater, unable to register the update listener due to pantheon client being null.");
            }

            this.pantheonClient.registerEventListener(this.updateListener);
        } else {
            logger.info("Update listener already registered, no-op");
        }

    }

    public synchronized void stop() {
        if (this.isActive.compareAndSet(true, false)) {
            if (this.pantheonClient != null) {
                this.pantheonClient.unregisterEventListener(this.updateListener);
            }
        } else {
            logger.info("Not currently active, no-op");
        }

    }

    public String getLastUpdate() {
        return (new Date(this.lastUpdated.get())).toString();
    }

    public long getDurationSinceLastUpdateMs() {
        return System.currentTimeMillis() - this.lastUpdated.get();
    }

    public int getNumberMissedCycles() {
        return 0;
    }

    public int getCoreThreads() {
        return this.isActive.get() && this.refreshExecutor != null && this.refreshExecutor instanceof ThreadPoolExecutor ? ((ThreadPoolExecutor)this.refreshExecutor).getCorePoolSize() : 0;
    }

    private static class LazyHolder {
        private static final String CORE_THREAD = "EurekaNotificationServerListUpdater.ThreadPoolSize";
        private static final String QUEUE_SIZE = "EurekaNotificationServerListUpdater.queueSize";
        private static final PantheonNotificationServerListUpdater.LazyHolder SINGLETON = new PantheonNotificationServerListUpdater.LazyHolder();
        private final DynamicIntProperty poolSizeProp = new DynamicIntProperty("EurekaNotificationServerListUpdater.ThreadPoolSize", 2);
        private final DynamicIntProperty queueSizeProp = new DynamicIntProperty("EurekaNotificationServerListUpdater.queueSize", 1000);
        private final ThreadPoolExecutor defaultServerListUpdateExecutor;
        private final Thread shutdownThread;

        private LazyHolder() {
            int corePoolSize = this.getCorePoolSize();
            this.defaultServerListUpdateExecutor = new ThreadPoolExecutor(corePoolSize, corePoolSize * 5, 0L, TimeUnit.NANOSECONDS, new ArrayBlockingQueue(this.queueSizeProp.get()), (new ThreadFactoryBuilder()).setNameFormat("EurekaNotificationServerListUpdater-%d").setDaemon(true).build());
            this.poolSizeProp.addCallback(new Runnable() {
                public void run() {
                    int corePoolSize = PantheonNotificationServerListUpdater.LazyHolder.this.getCorePoolSize();
                    PantheonNotificationServerListUpdater.LazyHolder.this.defaultServerListUpdateExecutor.setCorePoolSize(corePoolSize);
                    PantheonNotificationServerListUpdater.LazyHolder.this.defaultServerListUpdateExecutor.setMaximumPoolSize(corePoolSize * 5);
                }
            });
            this.shutdownThread = new Thread(new Runnable() {
                public void run() {
                    PantheonNotificationServerListUpdater.logger.info("Shutting down the Executor for EurekaNotificationServerListUpdater");

                    try {
                        PantheonNotificationServerListUpdater.LazyHolder.this.defaultServerListUpdateExecutor.shutdown();
                        Runtime.getRuntime().removeShutdownHook(PantheonNotificationServerListUpdater.LazyHolder.this.shutdownThread);
                    } catch (Exception var2) {
                    }

                }
            });
            Runtime.getRuntime().addShutdownHook(this.shutdownThread);
        }

        private int getCorePoolSize() {
            int propSize = this.poolSizeProp.get();
            return propSize > 0 ? propSize : 2;
        }
    }
}
