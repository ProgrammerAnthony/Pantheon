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



    public String getDataDir() {
        if (ObjectUtils.isEmpty(dataDir)) {
            dataDir = super.getDataDir();
        }
        return dataDir;
    }

    public Boolean isControllerCandidate() {
        if (ObjectUtils.isEmpty(isControllerCandidate)) {
            isControllerCandidate = super.isControllerCandidate();
        }
        return isControllerCandidate;
    }

    public Integer getNodeId() {
        if (ObjectUtils.isEmpty(nodeId)) {
            nodeId = super.getNodeId();
        }
        return nodeId;
    }

    public String getNodeIp() {
        if (ObjectUtils.isEmpty(nodeIp)) {
            nodeIp = super.getNodeIp();
        }
        return nodeIp;
    }

    public Integer getNodeInternTcpPort() {
        if (ObjectUtils.isEmpty(nodeInternTcpPort)) {
            nodeInternTcpPort = super.getNodeInternTcpPort();
        }
        return nodeInternTcpPort;
    }

    public  Integer getNodeClientHttpPort() {
        if (ObjectUtils.isEmpty(nodeClientHttpPort)) {
            nodeClientHttpPort = super.getNodeClientHttpPort();
        }
        return nodeClientHttpPort;
    }

    public Integer getNodeClientTcpPort() {
        if (ObjectUtils.isEmpty(nodeClientTcpPort)) {
            nodeClientTcpPort = super.getNodeClientTcpPort();
        }
        return nodeClientTcpPort;
    }

    public Integer getClusterNodeCount() {
        if (ObjectUtils.isEmpty(clusterNodeCount)) {
            clusterNodeCount = super.getClusterNodeCount();
        }
        return clusterNodeCount;
    }

    public String getControllerCandidateServers() {
        if (ObjectUtils.isEmpty(controllerCandidateServers)) {
            controllerCandidateServers = super.getControllerCandidateServers();
        }
        return controllerCandidateServers;
    }

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
