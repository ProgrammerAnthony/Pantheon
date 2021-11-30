package com.pantheon.server.config;

/**
 * @author Anthony
 * @create 2021/11/17
 * @desc Configuration information required by the Pantheon Server to operate.
 **/
public interface PantheonServerConfig {
    String getDataDir();

    Boolean isControllerCandidate();

    Integer getNodeId();

    String getNodeIp();

    Integer getNodeInternTcpPort();

    Integer getNodeClientHttpPort();

    Integer getNodeClientTcpPort();

    Integer getClusterNodeCount();

    String getControllerCandidateServers();

    Integer getHeartBeatCheckInterval();

    long getResponseCacheAutoExpirationInSeconds();

    long getResponseCacheUpdateIntervalMs();

    long getRetentionTimeInMSInDeltaQueue();

    long getDeltaRetentionTimerIntervalInMs();
}
