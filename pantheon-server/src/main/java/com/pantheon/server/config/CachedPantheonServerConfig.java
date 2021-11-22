package com.pantheon.server.config;

import com.pantheon.common.ObjectUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Anthony
 * @create 2021/11/20
 * @desc
 */
public class CachedPantheonServerConfig extends ArchaiusPantheonServerConfig {

    private String dataDir;
    private Boolean isControllerCandidate;
    private Integer nodeId;
    private String nodeIp;
    private Integer nodeInternTcpPort;
    private Integer nodeClientHttpPort;
    private Integer clusterNodeCount;
    private Integer heartBeatCheckInterval;
    private Integer nodeClientTcpPort;
    private String controllerCandidateServers;

    private static class Singleton {
        static CachedPantheonServerConfig instance = new CachedPantheonServerConfig();
    }

    public static CachedPantheonServerConfig getInstance() {
        return CachedPantheonServerConfig.Singleton.instance;
    }

    private CachedPantheonServerConfig() {
        super();
    }


    @Override
    public String getDataDir() {
        if (ObjectUtils.isEmpty(dataDir)) {
            dataDir = super.getDataDir();
        }
        return dataDir;
    }

    @Override
    public Boolean isControllerCandidate() {
        if (ObjectUtils.isEmpty(isControllerCandidate)) {
            isControllerCandidate = super.isControllerCandidate();
        }
        return isControllerCandidate;
    }

    @Override
    public Integer getNodeId() {
        if (ObjectUtils.isEmpty(nodeId)) {
            nodeId = super.getNodeId();
        }
        return nodeId;
    }

    @Override
    public String getNodeIp() {
        if (ObjectUtils.isEmpty(nodeIp)) {
            nodeIp = super.getNodeIp();
        }
        return nodeIp;
    }

    @Override
    public Integer getNodeInternTcpPort() {
        if (ObjectUtils.isEmpty(nodeInternTcpPort)) {
            nodeInternTcpPort = super.getNodeInternTcpPort();
        }
        return nodeInternTcpPort;
    }

     @Override
    public Integer getNodeClientHttpPort() {
        if (ObjectUtils.isEmpty(nodeClientHttpPort)) {
            nodeClientHttpPort = super.getNodeClientHttpPort();
        }
        return nodeClientHttpPort;
    }

    @Override
    public Integer getNodeClientTcpPort() {
        if (ObjectUtils.isEmpty(nodeClientTcpPort)) {
            nodeClientTcpPort = super.getNodeClientTcpPort();
        }
        return nodeClientTcpPort;
    }

    @Override
    public Integer getClusterNodeCount() {
        if (ObjectUtils.isEmpty(clusterNodeCount)) {
            clusterNodeCount = super.getClusterNodeCount();
        }
        return clusterNodeCount;
    }

    @Override
    public String getControllerCandidateServers() {
        if (ObjectUtils.isEmpty(controllerCandidateServers)) {
            controllerCandidateServers = super.getControllerCandidateServers();
        }
        return controllerCandidateServers;
    }

    @Override
    public Integer getHeartBeatCheckInterval() {
        if (ObjectUtils.isEmpty(heartBeatCheckInterval)) {
            heartBeatCheckInterval = super.getHeartBeatCheckInterval();
        }
        return heartBeatCheckInterval;
    }

    /**
     * get 'before candidates' avoid of nodes' duplicate connection
     *
     * @return
     */
    public List<String> getBeforeControllerCandidateServers() {
        List<String> beforeControllerCandidateServers = new ArrayList<String>();


        String nodeIp = getNodeIp();
        Integer nodeInternTcpPort = getNodeInternTcpPort();

        String controllerCandidateServers = getControllerCandidateServers();
        String[] controllerCandidateServersSplited = controllerCandidateServers.split(",");

        for (String controllerCandidateServer : controllerCandidateServersSplited) {
            String[] controllerCandidateServerSplited = controllerCandidateServer.split(":");
            String controllerCandidateIp = controllerCandidateServerSplited[0];
            Integer controllerCandidateInternTcpPort = Integer.valueOf(controllerCandidateServerSplited[1]);

            if (!controllerCandidateIp.equals(nodeIp) ||
                    !controllerCandidateInternTcpPort.equals(nodeInternTcpPort)) {
                beforeControllerCandidateServers.add(controllerCandidateServer);
            } else if (controllerCandidateIp.equals(nodeIp) &&
                    controllerCandidateInternTcpPort.equals(nodeInternTcpPort)) {
                break;
            }
        }

        return beforeControllerCandidateServers;
    }

    public List<String> getOtherControllerCandidateServers() {
        List<String> otherControllerCandidateServers = new ArrayList<String>();

        String nodeIp = getNodeIp();
        Integer nodeInternTcpPort = getNodeInternTcpPort();

        String controllerCandidateServers = getControllerCandidateServers();
        String[] controllerCandidateServersSplited = controllerCandidateServers.split(",");

        for (String controllerCandidateServer : controllerCandidateServersSplited) {
            String[] controllerCandidateServerSplited = controllerCandidateServer.split(":");
            String controllerCandidateIp = controllerCandidateServerSplited[0];
            Integer controllerCandidateInternTcpPort = Integer.valueOf(controllerCandidateServerSplited[1]);

            if (!controllerCandidateIp.equals(nodeIp) ||
                    !controllerCandidateInternTcpPort.equals(nodeInternTcpPort)) {
                otherControllerCandidateServers.add(controllerCandidateServer);
            }
        }

        return otherControllerCandidateServers;
    }
}
