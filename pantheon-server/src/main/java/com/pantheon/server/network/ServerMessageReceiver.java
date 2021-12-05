package com.pantheon.server.network;

import com.alibaba.fastjson.JSONObject;
import com.pantheon.common.MessageType;
import com.pantheon.common.lifecycle.Lifecycle;
import com.pantheon.common.entity.Request;
import com.pantheon.server.ServerNode;
import com.pantheon.server.node.Controller;
import com.pantheon.server.node.ControllerVote;
import com.pantheon.server.slot.SlotManager;
import com.pantheon.server.slot.registry.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;


public class ServerMessageReceiver extends Thread {

    static final Logger LOGGER = LoggerFactory.getLogger(ServerMessageReceiver.class);


    private ServerMessageReceiver() {

    }

    static class Singleton {
        static ServerMessageReceiver instance = new ServerMessageReceiver();
    }

    public static ServerMessageReceiver getInstance() {
        return Singleton.instance;
    }

    private LinkedBlockingQueue<ControllerVote> voteReceiveQueue =
            new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<ConcurrentHashMap<Integer, List<String>>> slotsAllocationReceiveQueue =
            new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<ConcurrentHashMap<Integer, List<String>>> slotsReplicaAllocationReceiveQueue =
            new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<ConcurrentHashMap<Integer, Integer>> replicaNodeIdsQueue =
            new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<List<String>> nodeSlotsQueue =
            new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<List<String>> nodeSlotsReplicasQueue =
            new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Integer> replicaNodeIdQueue =
            new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Integer> controllerNodeIdQueue =
            new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Request> replicaRequestQueue =
            new LinkedBlockingQueue<>();


    @Override
    public void run() {
        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        ServerNode serverNode = ServerNode.getInstance();
        while (serverNode.lifecycleState().equals(Lifecycle.State.INITIALIZED)||serverNode.lifecycleState().equals(Lifecycle.State.STARTED))  {
            try {
                ByteBuffer message = serverNetworkManager.takeMessage();
                int messageType = message.getInt();

                if (messageType == MessageType.VOTE) {
                    ControllerVote vote = new ControllerVote(message);
                    voteReceiveQueue.put(vote);
                    LOGGER.info("receive controller vote: " + vote);
                } else if (messageType == MessageType.SLOTS_ALLOCATION) {
                    int remaining = message.remaining();
                    byte[] bytes = new byte[remaining];
                    message.get(bytes);

                    String slotsAllocationJSON = new String(bytes);
                    ConcurrentHashMap<Integer, List<String>> slotsAllocation = JSONObject.parseObject(
                            slotsAllocationJSON, ConcurrentHashMap.class);
                    slotsAllocationReceiveQueue.put(slotsAllocation);

                    LOGGER.info("receive slots allocation : " + slotsAllocation);
                } else if (messageType == MessageType.NODE_SLOTS) {
                    int remaining = message.remaining();
                    byte[] bytes = new byte[remaining];
                    message.get(bytes);

                    String slotsListJSON = new String(bytes);
                    List<String> slotsList = JSONObject.parseObject(slotsListJSON, ArrayList.class);
                    nodeSlotsQueue.put(slotsList);

                    LOGGER.info("receive slots scope: " + slotsList);
                } else if (messageType == MessageType.UPDATE_NODE_SLOTS) {
                    int remaining = message.remaining();
                    byte[] bytes = new byte[remaining];
                    message.get(bytes);

                    String slotsListJSON = new String(bytes);
                    List<String> slotsList = JSONObject.parseObject(slotsListJSON, ArrayList.class);

                    SlotManager slotManager = SlotManager.getInstance();
                    slotManager.initSlots(slotsList);
                } else if (messageType == MessageType.SLOTS_REPLICA_ALLOCATION) {
                    int remaining = message.remaining();
                    byte[] bytes = new byte[remaining];
                    message.get(bytes);

                    String slotsReplicaAllocationJson = new String(bytes);
                    ConcurrentHashMap<Integer, List<String>> slotsReplicaAllocation = JSONObject.parseObject(
                            slotsReplicaAllocationJson, ConcurrentHashMap.class);
                    slotsReplicaAllocationReceiveQueue.put(slotsReplicaAllocation);

                    LOGGER.info("receive slots replica allocation : " + slotsReplicaAllocation);
                } else if (messageType == MessageType.NODE_SLOTS_REPLICAS) {
                    int remaining = message.remaining();
                    byte[] bytes = new byte[remaining];
                    message.get(bytes);

                    List<String> slotsReplicas = JSONObject.parseObject(new String(bytes), List.class);
                    nodeSlotsReplicasQueue.put(slotsReplicas);

                    LOGGER.info("receive current node's slots replica: " + slotsReplicas);
                } else if (messageType == MessageType.REPLICA_NODE_ID) {
                    Integer replicaNodeId = message.getInt();
                    replicaNodeIdQueue.put(replicaNodeId);
                    LOGGER.info("receive replica node id: " + replicaNodeId);
                } else if (messageType == MessageType.REFRESH_REPLICA_NODE_ID) {
                    Integer newReplicaNodeId = message.getInt();
                    SlotManager slotManager = SlotManager.getInstance();
                    slotManager.refreshReplicaNodeId(newReplicaNodeId);
                } else if (messageType == MessageType.REPLICA_REGISTER) {
                    //todo replica register info
                    Integer messageFlag = message.getInt();
                    Integer messageBodyLength = message.getInt();
                    Integer requestType = message.getInt();

//                    RegisterRequest registerRequest =
//                            RegisterRequest.deserialize(message);
//
//                    replicaRequestQueue.put(registerRequest);
//
//                    LOGGER.info("receive register request to replica: " + registerRequest);
                } else if (messageType == MessageType.REPLICA_HEARTBEAT) {
                    //todo replica heartbeat request
//                    Integer messageFlag = message.getInt();
//                    Integer messageBodyLength = message.getInt();
//                    Integer requestType = message.getInt();

//                    HeartbeatRequest heartbeatRequest =
//                            HeartbeatRequest.deserialize(message);
//
//                    replicaRequestQueue.put(heartbeatRequest);

//                    LOGGER.info("receive heartbeat request to replica: " + heartbeatRequest);
                } else if (messageType == MessageType.REPLICA_NODE_IDS) {
                    int remaining = message.remaining();
                    byte[] bytes = new byte[remaining];
                    message.get(bytes);

                    ConcurrentHashMap<Integer, Integer> replicaNodeIds = JSONObject.parseObject(
                            new String(bytes), ConcurrentHashMap.class);
                    replicaNodeIdsQueue.put(replicaNodeIds);

                    LOGGER.info("receive replica node ids: " + replicaNodeIds);
                } else if (messageType == MessageType.CONTROLLER_NODE_ID) {
                    Integer controllerNodeId = message.getInt();
                    controllerNodeIdQueue.put(controllerNodeId);
                    LOGGER.info("receive controller node id: " + controllerNodeId);
                } else if (messageType == MessageType.CHANGE_REPLICA_TO_SLOTS) {
                    //todo build mechanism of change replica to slots
//                    int remaining = message.remaining();
//                    byte[] bytes = new byte[remaining];
//                    message.get(bytes);
//
//                    List<String> slots = JSONObject.parseObject(new String(bytes), ArrayList.class);
//                    SlotManager slotManager = SlotManager.getInstance();
//                    slotManager.changeReplicaToSlots(slots);
                } else if (messageType == MessageType.REFRESH_REPLICA_SLOTS) {
                    //todo refresh replica slots
//                    int remaining = message.remaining();
//                    byte[] bytes = new byte[remaining];
//                    message.get(bytes);
//
//                    List<String> replicaSlots = JSONObject.parseObject(new String(bytes), ArrayList.class);
//                    SlotManager slotManager = SlotManager.getInstance();
//                    slotManager.refreshReplicaSlots(replicaSlots);
                } else if (messageType == MessageType.REQUEST_SLOTS_DATA) {
                    // take all of metadata from controller and sync to this node
                    Integer candidateNodeId = message.getInt();
                    Controller controller = Controller.getInstance();
                    controller.syncSlotsAllocation(candidateNodeId);
                    controller.syncSlotsReplicaAllocation(candidateNodeId);
                    controller.syncReplicaNodeIds(candidateNodeId);
                } else if (messageType == MessageType.TRANSFER_SLOTS) {
                    //todo transfer slots when  fail
                    Integer targetNodeId = message.getInt();

                    Integer bytesLength = message.getInt();
                    byte[] bytes = new byte[bytesLength];
                    message.get(bytes);
                    String slots = new String(bytes);

                    SlotManager slotManager = SlotManager.getInstance();
                    slotManager.transferSlots(targetNodeId, slots);
                } else if (messageType == MessageType.UPDATE_SLOTS) {
                    //todo update slots when others make a transfer
//                    Integer slotNo = message.getInt();
//
//                    int remaining = message.remaining();
//                    byte[] bytes = new byte[remaining];
//                    message.get(bytes);
//                    List<ServiceInstance> serviceInstances = JSONObject.parseObject(
//                            new String(bytes), ArrayList.class);
//
//                    SlotManager slotManager = SlotManager.getInstance();
//                    slotManager.updateSlotData(slotNo, serviceInstances);
                }
            } catch (Exception e) {
                LOGGER.error("receive message error......", e);
            }
        }
    }


    public ControllerVote takeVote() {
        try {
            return voteReceiveQueue.take();
        } catch (Exception e) {
            LOGGER.error("take vote message error......", e);
            return null;
        }
    }


    public ConcurrentHashMap<Integer, List<String>> takeSlotsAllocation() {
        try {
            return slotsAllocationReceiveQueue.take();
        } catch (Exception e) {
            LOGGER.error("take slots allocation message error......", e);
            return null;
        }
    }

    public ConcurrentHashMap<Integer, List<String>> takeSlotsReplicaAllocation() {
        try {
            return slotsReplicaAllocationReceiveQueue.take();
        } catch (Exception e) {
            LOGGER.error("take slots allocation message error......", e);
            return null;
        }
    }

    public List<String> takeNodeSlots() {
        try {
            return nodeSlotsQueue.take();
        } catch (Exception e) {
            LOGGER.error("take node slots message error......", e);
            return null;
        }
    }

    public List<String> takeNodeSlotsReplicas() {
        try {
            return nodeSlotsReplicasQueue.take();
        } catch (Exception e) {
            LOGGER.error("take node slots message error......", e);
            return null;
        }
    }

    public Integer takeReplicaNodeId() {
        try {
            return replicaNodeIdQueue.take();
        } catch (Exception e) {
            LOGGER.error("take node slots message error......", e);
            return null;
        }
    }

    public Request takeReplicaRequest() {
        try {
            return replicaRequestQueue.take();
        } catch (Exception e) {
            LOGGER.error("get replica request failed!!!", e);
            return null;
        }
    }

    public ConcurrentHashMap<Integer, Integer> takeReplicaNodeIds() {
        try {
            return replicaNodeIdsQueue.take();
        } catch (Exception e) {
            LOGGER.error("get replica node id failed!!!", e);
            return null;
        }
    }

    public Integer takeControllerNodeId() {
        try {
            return controllerNodeIdQueue.take();
        } catch (Exception e) {
            LOGGER.error("get controller node id failed!!!", e);
            return null;
        }
    }

}
