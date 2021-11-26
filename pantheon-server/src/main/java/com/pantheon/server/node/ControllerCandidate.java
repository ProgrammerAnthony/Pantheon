package com.pantheon.server.node;

import com.alibaba.fastjson.JSONObject;
import com.pantheon.common.MessageType;
import com.pantheon.common.ServerNodeRole;
import com.pantheon.common.component.Lifecycle;
import com.pantheon.server.ServerNode;
import com.pantheon.server.config.CachedPantheonServerConfig;
import com.pantheon.server.network.ServerMessageReceiver;
import com.pantheon.server.network.ServerNetworkManager;
import com.pantheon.server.persist.FilePersistUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Controller candidate node
 */
public class ControllerCandidate {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerCandidate.class);

    /**
     * filename of slot allocation
     */
    private static final String SLOTS_ALLOCATION_FILENAME = "slot_allocation";
    private static final String SLOTS_REPLICA_ALLOCATION_FILENAME = "slot_replica_allocation";
    private static final String REPLICA_NODE_IDS_FILENAME = "replica_node_ids";
    /**
     * expiration time of waiting all other master node to connect
     */
    private static final Long ALL_MASTER_NODE_CONNECT_CHECK_INTERVAL = 100L;

    private ControllerCandidate() {

    }

    static class Singleton {
        static ControllerCandidate instance = new ControllerCandidate();
    }

    public static ControllerCandidate getInstance() {
        return Singleton.instance;
    }

    private int voteRound = 1;
    //current vote
    private ControllerVote currentVote;
    private ConcurrentHashMap<Integer, List<String>> slotsAllocation;
    private ConcurrentHashMap<Integer, List<String>> slotsReplicaAllocation;
    private ConcurrentHashMap<Integer, Integer> replicaNodeIds;

    /**
     * elect controller within controller candidate
     *
     * @return
     */
    public ServerNodeRole electController() {
        Integer nodeId = CachedPantheonServerConfig.getInstance().getNodeId();

        //get all other controller candidate
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        List<RemoteServerNode> otherControllerCandidates =
                remoteServerNodeManager.getOtherControllerCandidates();
        LOGGER.info("other controller candidates : " + otherControllerCandidates);

        // first round election
        this.voteRound = 1;
        this.currentVote = new ControllerVote(nodeId, nodeId, voteRound);
        Integer controllerNodeId = startNextRoundVote(otherControllerCandidates);
        //first round failed to elect controller
        while (controllerNodeId == null) {
            controllerNodeId = startNextRoundVote(otherControllerCandidates);
        }

        // controller found!!
        if (nodeId.equals(controllerNodeId)) {
            return ServerNodeRole.CONTROLLER_NODE;
        } else {
            return ServerNodeRole.CONTROLLER_CANDIDATE_NODE;
        }
    }

    /**
     * 开启下一轮投票
     */
    private Integer startNextRoundVote(
            List<RemoteServerNode> otherControllerCandidates) {
        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        ServerMessageReceiver serverMessageReceiver = ServerMessageReceiver.getInstance();

        LOGGER.info("start round: " + voteRound + " votes......");

        Integer nodeId = CachedPantheonServerConfig.getInstance().getNodeId();

        // quorum num calc, if controller is 3，quorum = 3 / 2 + 1 = 2
        int candidateCount = (1 + otherControllerCandidates.size());
        int quorum = candidateCount / 2 + 1;


        List<ControllerVote> votes = new ArrayList<ControllerVote>();
        votes.add(currentVote);

        // initial vote to self
        ByteBuffer voteMessage = currentVote.getMessageByteBuffer();
        for (RemoteServerNode remoteNode : otherControllerCandidates) {
            Integer remoteNodeId = remoteNode.getNodeId();
            serverNetworkManager.sendMessage(remoteNodeId, voteMessage);
            LOGGER.info("send controller votes to : " + remoteNode);
        }

        // waiting for others' votes
        ServerNode serverNode = ServerNode.getInstance();
        while ((serverNode.lifecycleState().equals(Lifecycle.State.INITIALIZED)
                || serverNode.lifecycleState().equals(Lifecycle.State.STARTED))) {
            ControllerVote receivedVote = serverMessageReceiver.takeVote();

            if (receivedVote.getVoterNodeId() == null) {
                continue;
            }

            votes.add(receivedVote);
            LOGGER.info("received vote : " + receivedVote);

            // votes num > quorum leads to controller judge
            if (votes.size() >= quorum) {
                Integer judgedControllerNodeId =
                        getControllerFromVotes(votes, quorum);

                // controller found after judgement
                if (judgedControllerNodeId != null) {
                    if (votes.size() == candidateCount) {
                        LOGGER.info(" Controller Selected : " + judgedControllerNodeId + ", with all votes......");
                        return judgedControllerNodeId;
                    }
                    LOGGER.info("Controller Selected: " + judgedControllerNodeId + ", with not all votes......");
                } else {
                    LOGGER.info("Not yet affirm who is the Controller: " + votes);
                }
            }

            if (votes.size() == candidateCount) {
                //fail round: after all votes received ,but have not affirmed controller
                //start next round with a better controller node(the biggest one in all candidates)
                voteRound++;
                Integer betterControllerNodeId = getBetterControllerNodeId(votes);
                this.currentVote = new ControllerVote(nodeId, betterControllerNodeId, voteRound);

                LOGGER.info("fail round, create a better vote: " + currentVote);

                return null;
            }
        }

        return null;
    }

    /**
     * get controller node id from current votes
     *
     * @param votes
     * @return
     */
    private Integer getControllerFromVotes(List<ControllerVote> votes, int quorum) {
        // <1, 1>, <2, 1>, <3, 2>
        Map<Integer, Integer> voteCountMap = new HashMap<Integer, Integer>();

        for (ControllerVote vote : votes) {
            Integer controllerNodeId = vote.getControllerNodeId();

            Integer count = voteCountMap.get(controllerNodeId);
            if (count == null) {
                count = 0;
            }

            voteCountMap.put(controllerNodeId, ++count);
        }

        for (Map.Entry<Integer, Integer> voteCountEntry : voteCountMap.entrySet()) {
            if (voteCountEntry.getValue() >= quorum) {
                return voteCountEntry.getKey();
            }
        }

        return null;
    }

    /**
     * get least controller id
     *
     * @param votes
     * @return
     */
    private Integer getBetterControllerNodeId(List<ControllerVote> votes) {
        Integer betterControllerNodeId = 0;

        for (ControllerVote vote : votes) {
            Integer controllerNodeId = vote.getControllerNodeId();
            if (controllerNodeId > betterControllerNodeId) {
                betterControllerNodeId = controllerNodeId;
            }
        }

        return betterControllerNodeId;
    }


    public void waitForSlotsAllocation() {
        ServerMessageReceiver serverMessageReceiver = ServerMessageReceiver.getInstance();
        this.slotsAllocation = serverMessageReceiver.takeSlotsAllocation();
        String slotsAllocationJSON = JSONObject.toJSONString(slotsAllocation);
        byte[] slotsAllocationByteArray = slotsAllocationJSON.getBytes();
        FilePersistUtils.persist(slotsAllocationByteArray, SLOTS_ALLOCATION_FILENAME);
    }

    public Map<Integer, List<String>> getSlotsAllocation() {
        return slotsAllocation;
    }

    public ConcurrentHashMap<Integer, List<String>> getSlotsReplicaAllocation() {
        return slotsReplicaAllocation;
    }

    public ConcurrentHashMap<Integer, Integer> getReplicaNodeIds() {
        return replicaNodeIds;
    }

    public List<String> getServerAddresses() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();

        Integer nodeId = CachedPantheonServerConfig.getInstance().getNodeId();
        String ip = CachedPantheonServerConfig.getInstance().getNodeIp();
        Integer clientTcpPort = CachedPantheonServerConfig.getInstance().getNodeClientTcpPort();

        List<RemoteServerNode> servers = remoteServerNodeManager.getRemoteServerNodes();
        List<String> serverAddresses = new ArrayList<String>();

        for (RemoteServerNode server : servers) {
            serverAddresses.add(server.getNodeId() + ":" + server.getIp() + ":" + server.getClientPort());
        }
        serverAddresses.add(nodeId + ":" + ip + ":" + clientTcpPort);

        return serverAddresses;
    }

    public void waitForSlotsReplicaAllocation() {
        ServerMessageReceiver serverMessageReceiver = ServerMessageReceiver.getInstance();
        this.slotsReplicaAllocation = serverMessageReceiver.takeSlotsReplicaAllocation();
        String slotsReplicaAllocationJSON = JSONObject.toJSONString(slotsReplicaAllocation);
        byte[] slotsReplicaAllocationByteArray = slotsReplicaAllocationJSON.getBytes();
        FilePersistUtils.persist(slotsReplicaAllocationByteArray, SLOTS_REPLICA_ALLOCATION_FILENAME);
    }

    public void waitReplicaNodeIds() {
        ServerMessageReceiver serverMessageReceiver = ServerMessageReceiver.getInstance();
        this.replicaNodeIds = serverMessageReceiver.takeReplicaNodeIds();
        byte[] bytes = JSONObject.toJSONString(replicaNodeIds).getBytes();
        FilePersistUtils.persist(bytes, REPLICA_NODE_IDS_FILENAME);
    }

    /**
     * request slots data
     */
    public void requestSlotsData(Integer controllerNodeId) {
        Integer myNodeId = CachedPantheonServerConfig.getInstance().getNodeId();

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4);
        buffer.putInt(MessageType.REQUEST_SLOTS_DATA);
        buffer.putInt(myNodeId);

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        serverNetworkManager.sendMessage(controllerNodeId, buffer);
    }

}
