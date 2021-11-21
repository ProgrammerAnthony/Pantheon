package com.pantheon.server.node;

public class RemoteServerNode {

    /**
     * remote server's node id
     */
    private Integer nodeId;
    /**
     * whether a controller candidate
     */
    private Boolean isControllerCandidate;
    /**
     * whether a controller
     */
    private Boolean isController = false;
    /**
     * remote server's node ip
     */
    private String ip;
    /**
     * remote server's port for client connection
     */
    private Integer clientPort;

    public RemoteServerNode(Integer nodeId,
                            Boolean isControllerCandidate,
                            String ip,
                            Integer clientPort,
                            Boolean isContorller) {
        this.nodeId = nodeId;
        this.isControllerCandidate = isControllerCandidate;
        this.ip = ip;
        this.clientPort = clientPort;
        this.isController = isController();
    }

    public Integer getNodeId() {
        return nodeId;
    }

    public void setNodeId(Integer nodeId) {
        this.nodeId = nodeId;
    }

    public Boolean isControllerCandidate() {
        return isControllerCandidate;
    }

    public void setControllerCandidate(Boolean controllerCandidate) {
        isControllerCandidate = controllerCandidate;
    }

    public Boolean isController() {
        return isController;
    }

    public void setController(Boolean controller) {
        isController = controller;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getClientPort() {
        return clientPort;
    }

    public void setClientPort(Integer clientPort) {
        this.clientPort = clientPort;
    }

    @Override
    public String toString() {
        return "RemoteMasterNode{" +
                "nodeId=" + nodeId +
                ", isControllerCandidate=" + isControllerCandidate +
                ", ip='" + ip + '\'' +
                ", clientPort=" + clientPort +
                '}';
    }

}
