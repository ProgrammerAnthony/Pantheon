package com.pantheon.server.registry;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Anthony
 * @create 2021/11/30
 * @desc
 */
public interface ResponseCache {

    void invalidate(String appName);

    AtomicLong getVersionDelta();


    /**
     * Get the cached information about applications.
     *
     * <p>
     *
     * @param key the key for which the cached information needs to be obtained.
     * @return payload which contains information about the applications.
     */
    String get(Key key);

    /**
     * Get the compressed information about the applications.
     *
     * @param key the key for which the compressed cached information needs to be obtained.
     * @return compressed payload which contains information about the applications.
     */
    byte[] getGZIP(Key key);
}
