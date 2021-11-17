package com.pantheon.server.config;

/**
 * @author Anthony
 * @create 2021/11/17
 * @desc  Configuration information required by the Pantheon Server to operate.
 **/
public interface PantheonServerConfig {

    String getDataDir();

    String getControllerCandidateServers();

    int getHeartBeatInterval();

    String getNodeIp();

    int getNodeId();

    int getNodeInternTcpPort();

    int getNodeHttpPort();

    int getNodeClientTcpPort();

    boolean isControllerCandidate();

    int getClusterNodeCount();
}
