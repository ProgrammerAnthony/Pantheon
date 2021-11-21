package com.pantheon.server.node;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * save remote servers connecting with current node
 */
public class RemoteServerNodeManager {

    private RemoteServerNodeManager() {

    }

    static class Singleton {
        static RemoteServerNodeManager instance = new RemoteServerNodeManager();
    }

    public static RemoteServerNodeManager getInstance() {
        return Singleton.instance;
    }

    /**
     * save remote server nodes
     */
    private ConcurrentHashMap<Integer/**nodeId**/, RemoteServerNode> remoteServerNodes =
            new ConcurrentHashMap<Integer,RemoteServerNode>();


    public void addRemoteServerNode(RemoteServerNode remoteServerNode) {
        remoteServerNodes.put(remoteServerNode.getNodeId(), remoteServerNode);
    }

    /**
     * get all other controller candidates
     * @return
     */
    public List<RemoteServerNode> getOtherControllerCandidates() {
        List<RemoteServerNode> otherControllerCandidates = new ArrayList<RemoteServerNode>();

        for(RemoteServerNode remoteServerNode : remoteServerNodes.values()) {
            if(remoteServerNode.isControllerCandidate()) {
                otherControllerCandidates.add(remoteServerNode);
            }
        }

        return otherControllerCandidates;
    }

    /**
     * get all other master nodes
     * @return
     */
    public List<RemoteServerNode> getRemoteServerNodes() {
        return new ArrayList<>(remoteServerNodes.values());
    }

    /**
     * remove server node in memory
     * @param remoteNodeId
     */
    public void removeServerNode(Integer remoteNodeId) {
        remoteServerNodes.remove(remoteNodeId);
    }

    public RemoteServerNode getRemoteServerNode(Integer remoteNodeId) {
        return remoteServerNodes.get(remoteNodeId);
    }

    public boolean hasController() {
        for(RemoteServerNode remoteServerNode : remoteServerNodes.values()) {
            if(remoteServerNode.isController()) {
                return true;
            }
        }
        return false;
    }

    public RemoteServerNode getController() {
        for(RemoteServerNode remoteServerNode : remoteServerNodes.values()) {
            if(remoteServerNode.isController()) {
                return remoteServerNode;
            }
        }
        return null;
    }

}
