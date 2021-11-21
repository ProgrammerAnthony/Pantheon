package com.pantheon.server.node;

public class ControllerNode {

    private Integer nodeId;

    private ControllerNode() {

    }

    static class Singleton {
        static ControllerNode instance = new ControllerNode();
    }

    public static ControllerNode getInstance() {
        return Singleton.instance;
    }

    public Integer getNodeId() {
        return nodeId;
    }

    public static void setNodeId(Integer nodeId) {
        getInstance().nodeId = nodeId;
    }

    public static Boolean isControllerNode(Integer nodeId) {
        return getInstance().nodeId.equals(nodeId);
    }

}
