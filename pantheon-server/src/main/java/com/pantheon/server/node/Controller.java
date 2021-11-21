package com.pantheon.server.node;

import com.alibaba.fastjson.JSONObject;
import com.pantheon.common.MessageType;
import com.pantheon.common.ServiceState;
import com.pantheon.server.ServerController;
import com.pantheon.server.config.ArchaiusPantheonServerConfig;
import com.pantheon.server.config.CachedPantheonServerConfig;
import com.pantheon.server.network.ServerNetworkManager;
import com.pantheon.server.persist.FilePersistUtils;
import com.pantheon.server.slot.SlotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Controller node
 */
public class Controller {

    private static final Logger LOGGER = LoggerFactory.getLogger(Controller.class);

    /**
     * total slots count
     */
    private static final int SLOTS_COUNT = 16384;
    /**
     * file name of slots allocation
     */
    private static final String SLOTS_ALLOCATION_FILENAME = "slots_allocation";
    /**
     * file name of the replica of slots allocation
     */
    private static final String SLOTS_REPLICA_ALLOCATION_FILENAME = "slots_replica_allocation";
    private static final String REPLICA_NODE_IDS_FILENAME = "replica_node_ids";
    /**
     * file name of node slots
     */
    private static final String NODE_SLOTS_FILENAME = "node_slots";
    private static final String NODE_SLOTS_REPLICAS_FILENAME = "node_slots_replicas";

    private Controller() {

    }

    static class Singleton {
        static Controller instance = new Controller();
    }

    public static Controller getInstance() {
        return Singleton.instance;
    }

    /**
     * slots allocation
     */
    private ConcurrentHashMap<Integer/*nodeId*/, List<String>/*slots scope*/> slotsAllocation =
            new ConcurrentHashMap<Integer, List<String>>();
    /**
     * replica of slots allocation
     */
    private ConcurrentHashMap<Integer/*nodeId*/, List<String>/*slots replica scope*/> slotsReplicaAllocation =
            new ConcurrentHashMap<>();
    /**
     * nodeId and location of my replica
     */
    private ConcurrentHashMap<Integer/*nodeId*/, Integer/*replica node id*/> replicaNodeIds =
            new ConcurrentHashMap<>();
    private long startTimestamp = new Date().getTime();

    /**
     * allocate all slots to master node
     */
    public void allocateSlots() {
        executeSlotsAllocation();

        executeSlotsReplicaAllocation();

        if (!persistSlotsAllocation()) {
            ServerController.setServiceState(ServiceState.START_FAILED);
            return;
        }
        if (!persistSlotsReplicaAllocation()) {
            ServerController.setServiceState(ServiceState.START_FAILED);
            return;
        }
        if (!persistReplicaNodeIds()) {
            ServerController.setServiceState(ServiceState.START_FAILED);
            return;
        }

        // after the slots' allocation, save it to memory and disk, sync to other node
        syncSlotsAllocation();
        syncSlotsReplicaAllocation();
        syncReplicaNodeIds();

        initSlots();
        initSlotsReplicas();
        initReplicaNodeId();

        //controller is responsible for sending node slots to other candidates and other master nodes
        sendNodeSlots();

        //these nodes will init and persist slots' scope and slots scopes replica
        sendNodeSlotsReplicas();
        sendReplicaNodeId();
    }

    /**
     * controller node initialization
     */
    public void initControllerNode() {
        Integer nodeId = CachedPantheonServerConfig.getInstance().getNodeId();
        ControllerNode.setNodeId(nodeId);
    }

    /**
     * controller node's replica slots initialization
     */
    private void initReplicaNodeId() {
        Integer nodeId = CachedPantheonServerConfig.getInstance().getNodeId();
        Integer replicaNodeId = replicaNodeIds.get(nodeId);
        SlotManager slotManager = SlotManager.getInstance();
        slotManager.initReplicaNodeId(replicaNodeId);
    }

    /**
     * calculate slots allocation with {@link #SLOTS_COUNT} and master node count
     */
    private void executeSlotsAllocation() {
        // current node id
        ArchaiusPantheonServerConfig config = CachedPantheonServerConfig.getInstance();
        Integer myNodeId = config.getNodeId();

        //get master node count
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> remoteMasterNodes =
                remoteServerNodeManager.getRemoteServerNodes();
        int totalMasterNodeCount = remoteMasterNodes.size() + 1;
        // allocate slots per node
        int slotsPerMasterNode = SLOTS_COUNT / totalMasterNodeCount;
        int remainSlotsCount = SLOTS_COUNT - slotsPerMasterNode * totalMasterNodeCount;

        Integer nextStartSlot = 1;
        Integer nextEndSlot = nextStartSlot - 1 + slotsPerMasterNode;


        for (RemoteServerNode remoteMasterNode : remoteMasterNodes) {
            List<String> slotsList = new ArrayList<String>();
            slotsList.add(nextStartSlot + "," + nextEndSlot);
            slotsAllocation.put(remoteMasterNode.getNodeId(), slotsList);
            nextStartSlot = nextEndSlot + 1;
            nextEndSlot = nextStartSlot - 1 + slotsPerMasterNode;
        }

        List<String> slotsList = new ArrayList<String>();
        slotsList.add(nextStartSlot + "," + (nextEndSlot + remainSlotsCount));
        slotsAllocation.put(myNodeId, slotsList);

        LOGGER.info("allocate slots complete：" + slotsAllocation);
    }

    /**
     * get node id of all nodes and then allocate slots replica
     */
    private void executeSlotsReplicaAllocation() {
        // get node id of all nodes
        List<Integer> nodeIds = new ArrayList<Integer>();

        Integer myNodeId = CachedPantheonServerConfig.getInstance().getNodeId();
        nodeIds.add(myNodeId);

        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> remoteMasterNodes =
                remoteServerNodeManager.getRemoteServerNodes();
        for (RemoteServerNode remoteServerNode : remoteMasterNodes) {
            nodeIds.add(remoteServerNode.getNodeId());
        }

        // allocate slots replica with a random server node
        Random random = new Random();

        for (Map.Entry<Integer, List<String>> nodeSlots : slotsAllocation.entrySet()) {
            Integer nodeId = nodeSlots.getKey();
            List<String> slotsList = nodeSlots.getValue();

            Integer replicaNodeId = null;
            boolean hasDecidedReplicaNode = false;

            while (!hasDecidedReplicaNode) {
                replicaNodeId = nodeIds.get(random.nextInt(nodeIds.size()));
                if (!replicaNodeId.equals(nodeId)) {
                    hasDecidedReplicaNode = true;
                }
            }

            List<String> slotsReplicas = slotsReplicaAllocation.get(replicaNodeId);

            if (slotsReplicas == null) {
                slotsReplicas = new ArrayList<String>();
                slotsReplicaAllocation.put(replicaNodeId, slotsReplicas);
            }
            slotsReplicas.addAll(slotsList);

            replicaNodeIds.put(nodeId, replicaNodeId);
        }

        LOGGER.info("allocate slots replica complete：" + slotsReplicaAllocation);
    }


    private Boolean persistSlotsAllocation() {
        String slotsAllocationJSON = JSONObject.toJSONString(slotsAllocation);
        byte[] slotsAllocationByteArray = slotsAllocationJSON.getBytes();
        return FilePersistUtils.persist(slotsAllocationByteArray, SLOTS_ALLOCATION_FILENAME);
    }

    private Boolean persistSlotsReplicaAllocation() {
        String slotsReplicaAllocationJSON = JSONObject.toJSONString(slotsReplicaAllocation);
        byte[] slotsReplicaAllocationByteArray = slotsReplicaAllocationJSON.getBytes();
        return FilePersistUtils.persist(slotsReplicaAllocationByteArray, SLOTS_REPLICA_ALLOCATION_FILENAME);
    }

    private Boolean persistReplicaNodeIds() {
        byte[] bytes = JSONObject.toJSONString(replicaNodeIds).getBytes();
        return FilePersistUtils.persist(bytes, REPLICA_NODE_IDS_FILENAME);
    }

    /**
     * persist node slots into disk
     *
     * @return
     */
    private Boolean persistNodeSlots() {
        Integer nodeId = CachedPantheonServerConfig.getInstance().getNodeId();
        List<String> slotsList = slotsAllocation.get(nodeId);
        byte[] bytes = JSONObject.toJSONString(slotsList).getBytes();
        return FilePersistUtils.persist(bytes, NODE_SLOTS_FILENAME);
    }

    /**
     * persist node slots replica into disk
     *
     * @return
     */
    private Boolean persistNodeSlotsReplicas() {
        Integer nodeId = CachedPantheonServerConfig.getInstance().getNodeId();
        List<String> slotsReplicas = slotsReplicaAllocation.get(nodeId);
        byte[] bytes = JSONObject.toJSONString(slotsReplicas).getBytes();
        return FilePersistUtils.persist(bytes, NODE_SLOTS_REPLICAS_FILENAME);
    }

    /**
     * sync slots allocation to other controller candidates
     */
    public void syncSlotsAllocation() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> otherControllerCandidates =
                remoteServerNodeManager.getOtherControllerCandidates();

        byte[] slotsAllocationByteArray = JSONObject
                .toJSONString(slotsAllocation).getBytes();

        ByteBuffer slotsAllocationByteBuffer =
                ByteBuffer.allocate(4 + slotsAllocationByteArray.length);
        slotsAllocationByteBuffer.putInt(MessageType.SLOTS_ALLOCATION);
        slotsAllocationByteBuffer.put(slotsAllocationByteArray);

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        for (RemoteServerNode controllerCandidate : otherControllerCandidates) {
            serverNetworkManager.sendMessage(controllerCandidate.getNodeId(), slotsAllocationByteBuffer);
        }

        LOGGER.info("sync slots allocation data to other node already......");
    }

    public void syncSlotsReplicaAllocation() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> otherControllerCandidates =
                remoteServerNodeManager.getOtherControllerCandidates();

        byte[] bytes = JSONObject.toJSONString(replicaNodeIds).getBytes();

        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + bytes.length);
        byteBuffer.putInt(MessageType.REPLICA_NODE_IDS);
        byteBuffer.put(bytes);

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        for (RemoteServerNode controllerCandidate : otherControllerCandidates) {
            serverNetworkManager.sendMessage(controllerCandidate.getNodeId(), byteBuffer);
        }

        LOGGER.info("sync slots allocation data to other node already......");
    }

    public void syncReplicaNodeIds() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> otherControllerCandidates =
                remoteServerNodeManager.getOtherControllerCandidates();

        byte[] bytes = JSONObject.toJSONString(slotsReplicaAllocation).getBytes();

        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + bytes.length);
        byteBuffer.putInt(MessageType.SLOTS_REPLICA_ALLOCATION);
        byteBuffer.put(bytes);

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        for (RemoteServerNode controllerCandidate : otherControllerCandidates) {
            serverNetworkManager.sendMessage(controllerCandidate.getNodeId(), byteBuffer);
        }

        LOGGER.info("sync slots replica id data to other node already......");
    }

    public void syncSlotsAllocation(Integer candidateNodeId) {
        byte[] bytes = JSONObject
                .toJSONString(slotsAllocation).getBytes();

        ByteBuffer buffer =
                ByteBuffer.allocate(4 + bytes.length);
        buffer.putInt(MessageType.SLOTS_ALLOCATION);
        buffer.put(bytes);

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        serverNetworkManager.sendMessage(candidateNodeId, buffer);
    }

    public void syncSlotsReplicaAllocation(Integer candidateNodeId) {
        byte[] bytes = JSONObject.toJSONString(replicaNodeIds).getBytes();

        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + bytes.length);
        byteBuffer.putInt(MessageType.REPLICA_NODE_IDS);
        byteBuffer.put(bytes);

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        serverNetworkManager.sendMessage(candidateNodeId, byteBuffer);
    }

    public void syncReplicaNodeIds(Integer candidateNodeId) {
        byte[] bytes = JSONObject.toJSONString(slotsReplicaAllocation).getBytes();

        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + bytes.length);
        byteBuffer.putInt(MessageType.SLOTS_REPLICA_ALLOCATION);
        byteBuffer.put(bytes);

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        serverNetworkManager.sendMessage(candidateNodeId, byteBuffer);
    }


    private void initSlots() {
        SlotManager slotManager = SlotManager.getInstance();

        Integer nodeId = CachedPantheonServerConfig.getInstance().getNodeId();
        List<String> slotsList = slotsAllocation.get(nodeId);
        slotManager.initSlots(slotsList);
    }

    private void initSlotsReplicas() {
        SlotManager slotManager = SlotManager.getInstance();
        Integer nodeId = CachedPantheonServerConfig.getInstance().getNodeId();
        List<String> slotsReplicas = slotsReplicaAllocation.get(nodeId);
        slotManager.initSlotsReplicas(slotsReplicas, true);
    }


    private void sendNodeSlots() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> nodes =
                remoteServerNodeManager.getRemoteServerNodes();

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        for (RemoteServerNode node : nodes) {
            List<String> slotsList = slotsAllocation.get(node.getNodeId());

            byte[] bytes = JSONObject.toJSONString(slotsList).getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(4 + bytes.length);
            buffer.putInt(MessageType.NODE_SLOTS);
            buffer.put(bytes);

            serverNetworkManager.sendMessage(node.getNodeId(), buffer);
        }

        LOGGER.info("send slots allocation to other node successfully......");
    }

    public void sendNodeSlots(Integer nodeId) {
        List<String> slotsList = slotsAllocation.get(nodeId);

        byte[] bytes = JSONObject.toJSONString(slotsList).getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(4 + bytes.length);
        buffer.putInt(MessageType.UPDATE_NODE_SLOTS);
        buffer.put(bytes);

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        serverNetworkManager.sendMessage(nodeId, buffer);
    }

    private void sendNodeSlotsReplicas() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> nodes = remoteServerNodeManager.getRemoteServerNodes();

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        for (RemoteServerNode node : nodes) {
            List<String> slotsReplicas = slotsReplicaAllocation.get(node.getNodeId());
            if (slotsReplicas == null || slotsReplicas.size() == 0) {
                slotsReplicas = new ArrayList<String>();
            }

            byte[] bytes = JSONObject.toJSONString(slotsReplicas).getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(4 + bytes.length);
            buffer.putInt(MessageType.NODE_SLOTS_REPLICAS);
            buffer.put(bytes);

            serverNetworkManager.sendMessage(node.getNodeId(), buffer);
        }

        LOGGER.info("send node slots replica to other node successfully......");
    }

    private void sendReplicaNodeId() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> nodes =
                remoteServerNodeManager.getRemoteServerNodes();

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        for (RemoteServerNode node : nodes) {
            Integer replicaNodeId = replicaNodeIds.get(node.getNodeId());

            ByteBuffer buffer = ByteBuffer.allocate(4 + 4);
            buffer.putInt(MessageType.REPLICA_NODE_ID);
            buffer.putInt(replicaNodeId);

            serverNetworkManager.sendMessage(node.getNodeId(), buffer);
        }

        LOGGER.info("send replica node id to other node successfully......");
    }

    public void sendReplicaNodeId(Integer nodeId) {
        Integer replicaNodeId = replicaNodeIds.get(nodeId);

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4);
        buffer.putInt(MessageType.UPDATE_REPLICA_NODE_ID);
        buffer.putInt(replicaNodeId);

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        serverNetworkManager.sendMessage(nodeId, buffer);
    }

    public void transferSlots(Integer sourceNodeId, Integer targetNodeId, String slots) {
        byte[] bytes = slots.getBytes();

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 4 + bytes.length);
        buffer.putInt(MessageType.TRANSFER_SLOTS);
        buffer.putInt(targetNodeId);
        buffer.putInt(bytes.length);
        buffer.put(bytes);

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        serverNetworkManager.sendMessage(sourceNodeId, buffer);
    }

    public void sendControllerNodeId() {
        Integer nodeId = CachedPantheonServerConfig.getInstance().getNodeId();

        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> nodes =
                remoteServerNodeManager.getRemoteServerNodes();

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        for (RemoteServerNode node : nodes) {
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4);
            buffer.putInt(MessageType.CONTROLLER_NODE_ID);
            buffer.putInt(nodeId);
            serverNetworkManager.sendMessage(node.getNodeId(), buffer);
        }

        LOGGER.info("send controller id successfully......");
    }

    public Map<Integer, List<String>> getSlotsAllocation() {
        return slotsAllocation;
    }

    public void setSlotsAllocation(ConcurrentHashMap<Integer, List<String>> slotsAllocation) {
        this.slotsAllocation = slotsAllocation;
    }

    public ConcurrentHashMap<Integer, List<String>> getSlotsReplicaAllocation() {
        return slotsReplicaAllocation;
    }

    public void setSlotsReplicaAllocation(ConcurrentHashMap<Integer, List<String>> slotsReplicaAllocation) {
        this.slotsReplicaAllocation = slotsReplicaAllocation;
    }

    public ConcurrentHashMap<Integer, Integer> getReplicaNodeIds() {
        return replicaNodeIds;
    }

    public void setReplicaNodeIds(ConcurrentHashMap<Integer, Integer> replicaNodeIds) {
        this.replicaNodeIds = replicaNodeIds;
    }

    /**
     * get server node list
     *
     * @return
     */
    public List<String> getServerAddresses() {
        Integer nodeId = CachedPantheonServerConfig.getInstance().getNodeId();
        String ip = CachedPantheonServerConfig.getInstance().getNodeIp();
        Integer clientTcpPort = CachedPantheonServerConfig.getInstance().getNodeClientTcpPort();

        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> servers = remoteServerNodeManager.getRemoteServerNodes();
        List<String> serverAddresses = new ArrayList<String>();

        for (RemoteServerNode server : servers) {
            serverAddresses.add(server.getNodeId() + ":" + server.getIp() + ":" + server.getClientPort());
        }
        serverAddresses.add(nodeId + ":" + ip + ":" + clientTcpPort);

        return serverAddresses;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }
}
