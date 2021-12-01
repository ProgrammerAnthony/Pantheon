package com.pantheon.server.registry;

import com.alibaba.fastjson.JSON;
import com.google.common.cache.*;
import com.pantheon.client.appinfo.Application;
import com.pantheon.client.appinfo.Applications;
import com.pantheon.server.config.CachedPantheonServerConfig;
import com.pantheon.server.config.PantheonServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

/**
 * @author Anthony
 * @create 2021/11/30
 * @desc The class that is responsible for caching registry information that will be
 * queried by the clients.
 *
 * <p>
 * The cache is maintained in compressed and non-compressed form for three
 * categories of requests - all applications, delta changes and for individual
 * applications. The compressed form is probably the most efficient in terms of
 * network traffic especially when querying all applications.
 */
public class ResponseCacheImpl implements ResponseCache {
    private final PantheonServerConfig serverConfig;
    private final InstanceRegistryImpl instanceRegistryImpl;
    private static final Logger logger = LoggerFactory.getLogger(ResponseCacheImpl.class);

    public static final String ALL_APPS = "ALL_APPS";
    public static final String ALL_APPS_DELTA = "ALL_APPS_DELTA";
    private static final String EMPTY_PAYLOAD = "";
    private final AtomicLong versionDelta = new AtomicLong(0);
    //map实现readOnlyCacheMap
    private final ConcurrentMap<Key, Value> readOnlyCacheMap = new ConcurrentHashMap<Key, Value>();
    //guava缓存
    private final LoadingCache<Key, Value> readWriteCacheMap;
    private final java.util.Timer timer = new java.util.Timer("Pantheon-CacheFillTimer", true);


    public ResponseCacheImpl(CachedPantheonServerConfig serverConfig, InstanceRegistryImpl instanceRegistryImpl) {
        this.serverConfig = serverConfig;
        this.instanceRegistryImpl = instanceRegistryImpl;
        //default 30
        long responseCacheUpdateIntervalMs = serverConfig.getResponseCacheUpdateIntervalMs();
        this.readWriteCacheMap =
                CacheBuilder.newBuilder().initialCapacity(1000)
                        //default 180s
                        .expireAfterWrite(serverConfig.getResponseCacheAutoExpirationInSeconds(), TimeUnit.SECONDS)
                        .removalListener(new RemovalListener<Key, Value>() {
                            @Override
                            public void onRemoval(RemovalNotification<Key, Value> notification) {
                                Key removedKey = notification.getKey();
                            }
                        })
//If the cached information is not available it is generated on the first
//request. After the first request, the information is then updated periodically by a background thread.
                        .build(new CacheLoader<Key, Value>() {
                            @Override
                            public Value load(Key key) throws Exception {
                                Value value = generatePayload(key);
                                return value;
                            }
                        });

        timer.schedule(getCacheUpdateTask(),
                new Date(((System.currentTimeMillis() / responseCacheUpdateIntervalMs) * responseCacheUpdateIntervalMs)
                        + responseCacheUpdateIntervalMs),
                responseCacheUpdateIntervalMs);
    }

    private Value generatePayload(Key key) {
        String payload;
        if (ALL_APPS.equals(key.getName())) {
            payload = getPayLoad(instanceRegistryImpl.getApplications());
        } else if (ALL_APPS_DELTA.equals(key.getName())) {
            versionDelta.incrementAndGet();
            payload = getPayLoad(instanceRegistryImpl.getApplicationDeltas());
        } else {
            payload = getPayLoad(instanceRegistryImpl.getApplication(key.getName()));
        }
        return new Value(payload);
    }

    private String getPayLoad(Applications applications) {
        return JSON.toJSONString(applications);
    }


    /**
     * Generate pay load with both JSON and XML formats for a given application.
     */
    private String getPayLoad(Application app) {
        if (app == null) {
            return EMPTY_PAYLOAD;
        }
        return JSON.toJSONString(app);
    }

    /**
     * Invalidate the cache of a particular application.
     *
     * @param appName the application name of the application.
     */
    @Override
    public void invalidate(String appName) {
        invalidate(
                new Key(ALL_APPS, Key.ACCEPT.FULL),
                new Key(ALL_APPS, Key.ACCEPT.COMPACT),
                new Key(ALL_APPS_DELTA, Key.ACCEPT.FULL),
                new Key(ALL_APPS_DELTA, Key.ACCEPT.COMPACT)
        );
    }

    /**
     * Invalidate the cache information given the list of keys.
     *
     * @param keys the list of keys for which the cache information needs to be invalidated.
     */
    public void invalidate(Key... keys) {
        for (Key key : keys) {
            logger.debug("Invalidating the response cache key : {} {} ",
                    "Application", key.getName());

            readWriteCacheMap.invalidate(key);
        }
    }

    /**
     * Gets the version number of the cached data.
     *
     * @return teh version number of the cached data.
     */
    @Override
    public AtomicLong getVersionDelta() {
        return versionDelta;
    }

    @Override
    public String get(Key key) {
        Value payload = getValue(key);
        if (payload == null || payload.getPayload().equals(EMPTY_PAYLOAD)) {
            return null;
        } else {
            return payload.getPayload();
        }
    }

    /**
     * Get the payload in both compressed and uncompressed form.
     */
    Value getValue(final Key key) {
        Value payload = null;
        try {
            //first cache
            final Value currentPayload = readOnlyCacheMap.get(key);
            if (currentPayload != null) {
                payload = currentPayload;
            } else {
                //secondary cache
                payload = readWriteCacheMap.get(key);
                readOnlyCacheMap.put(key, payload);
            }
        } catch (Throwable t) {
            logger.error("Cannot get value for key :" + key, t);
        }
        return payload;
    }

    @Override
    public byte[] getGZIP(Key key) {
        return new byte[0];
    }


    /**
     * transfer cache data from readWriteCacheMap to readOnlyCacheMap
     */
    private TimerTask getCacheUpdateTask() {
        return new TimerTask() {
            @Override
            public void run() {
                logger.debug("Updating the client cache from response cache");
                for (Key key : readOnlyCacheMap.keySet()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Updating the client cache from response cache for key : {}", key.getName());
                    }
                    try {
                        Value cacheValue = readWriteCacheMap.get(key);
                        Value currentCacheValue = readOnlyCacheMap.get(key);
                        if (cacheValue != currentCacheValue) {
                            readOnlyCacheMap.put(key, cacheValue);
                        }
                    } catch (Throwable th) {
                        logger.error("Error while updating the client cache from response cache", th);
                    }
                }
            }
        };
    }


    /**
     * The class that stores payload in both compressed and uncompressed form.
     */
    public class Value {
        private final String payload;
        private byte[] gzipped;

        public Value(String payload) {
            this.payload = payload;
            if (!EMPTY_PAYLOAD.equals(payload)) {
                try {
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    GZIPOutputStream out = new GZIPOutputStream(bos);
                    byte[] rawBytes = payload.getBytes();
                    out.write(rawBytes);
                    // Finish creation of gzip file
                    out.finish();
                    out.close();
                    bos.close();
                    gzipped = bos.toByteArray();
                } catch (IOException e) {
                    gzipped = null;
                }
            } else {
                gzipped = null;
            }
        }

        public String getPayload() {
            return payload;
        }

        public byte[] getGzipped() {
            return gzipped;
        }
    }

    /**
     * Get the number of items in the response cache.
     *
     * @return int value representing the number of items in response cache.
     */
    public int getCurrentSize() {
        return readWriteCacheMap.asMap().size();
    }
}
