package com.pantheon.server.slot;

import com.alibaba.fastjson.JSONObject;
import com.pantheon.common.MessageType;
import com.pantheon.server.network.ServerMessageReceiver;
import com.pantheon.server.network.ServerNetworkManager;
import com.pantheon.server.persist.FilePersistUtils;
import com.pantheon.server.slot.registry.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * slot data manage
 */
public class SlotManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SlotManager.class);

    /**
     * file name for node slots
     */
    public static final String NODE_SLOTS_FILENAME = "node_slots";
    /**
     * file name for node replica
     */
    public static final String NODE_SLOTS_REPLICAS_FILENAME = "node_slots_replicas";
    public static final Integer SLOT_COUNT = 16384;

    private SlotManager() {

    }

    static class Singleton {
        static SlotManager instance = new SlotManager();
    }

    public static SlotManager getInstance() {
        return Singleton.instance;
    }

    /**
     * slots in current node
     */
    private Slots slots = new Slots();
    /**
     * slots replica in current node
     */
    private Map<String/*slotScope*/, SlotsReplica> slotsReplicas = new ConcurrentHashMap<>();


    public void initSlots(List<String> slotsList) {
        if (slotsList == null) {
            ServerMessageReceiver serverMessageReceiver = ServerMessageReceiver.getInstance();
            slotsList = serverMessageReceiver.takeNodeSlots();
        }

        for (String slotScope : slotsList) {
            slots.init(slotScope);
        }
        FilePersistUtils.persist(JSONObject.toJSONString(slotsList).getBytes(), NODE_SLOTS_FILENAME);

        LOGGER.info("initialization of current node slots allocation finish......");
    }


    public void initSlotsReplicas(List<String> slotScopes, boolean isController) {
        ServerMessageReceiver serverMessageReceiver = ServerMessageReceiver.getInstance();
        if (slotScopes == null && !isController) {
            slotScopes = serverMessageReceiver.takeNodeSlotsReplicas();
        } else if (slotScopes == null && isController) {
            return;
        }

        for (String slotScope : slotScopes) {
            SlotsReplica slotsReplica = new SlotsReplica();
            slotsReplica.init(slotScope);
            slotsReplicas.put(slotScope, slotsReplica);
        }

        byte[] bytes = JSONObject.toJSONString(slotScopes).getBytes();
        FilePersistUtils.persist(bytes, NODE_SLOTS_REPLICAS_FILENAME);
        LOGGER.info("initialization of current node slots replica allocation finish......");
    }

    /**
     * init replica node id
     */
    public void initReplicaNodeId(Integer replicaNodeId) {
        ServerMessageReceiver serverMessageReceiver = ServerMessageReceiver.getInstance();
        if(replicaNodeId == null) {
            replicaNodeId = serverMessageReceiver.takeReplicaNodeId();
        }
        slots.setReplicaNodeId(replicaNodeId);
        LOGGER.info("init replica node id complete：" + replicaNodeId);
    }

    public void refreshReplicaNodeId(Integer newReplicaNodeId) {
        LOGGER.info("副本节点id进行刷新，老节点id：" + slots.getReplicaNodeId() + "，新节点id：" + newReplicaNodeId);
        slots.setReplicaNodeId(newReplicaNodeId);
    }

    /**
     * get slot
     *
     * @param serviceName
     * @return
     */
    public Slot getSlot(String serviceName) {
        return slots.getSlot(routeSlot(serviceName));
    }

    /**
     * get slot replica
     *
     * @param serviceName
     * @return
     */
    public Slot getSlotReplica(String serviceName) {
        Integer slotNo = routeSlot(serviceName);

        SlotsReplica slotsReplica = null;

        for (String slotScope : slotsReplicas.keySet()) {
            Integer startSlot = Integer.valueOf(slotScope.split(",")[0]);
            Integer endSlot = Integer.valueOf(slotScope.split(",")[1]);

            if (slotNo >= startSlot && slotNo <= endSlot) {
                slotsReplica = slotsReplicas.get(slotScope);
                break;
            }
        }

        return slotsReplica.getSlot(slotNo);
    }

    /**
     * slots replica regularize to slots
     *
     * @param replicaSlotsList
     */
    public void changeReplicaToSlots(List<String> replicaSlotsList) {
        // 把这些slots从当前节点管理的副本槽位里拿出来
        // 把拿出来的副本slots都转移到正式的槽位数据集合里去
        for (String replicaSlots : replicaSlotsList) {
            SlotsReplica slotsReplica = slotsReplicas.get(replicaSlots);
            ConcurrentHashMap<Integer, Slot> _slots = slotsReplica.getSlots();
            for (Integer slotNo : _slots.keySet()) {
                Slot slot = _slots.get(slotNo);
                slots.putSlot(slotNo, slot);
            }
        }

        for (String replicaSlots : replicaSlotsList) {
            slotsReplicas.remove(replicaSlots);
        }

        LOGGER.info("slots replica （" + replicaSlotsList + "）regularized......");
    }

    /**
     * refresh replica slots
     *
     * @param replicaSlotsList
     */
    public void refreshReplicaSlots(List<String> replicaSlotsList) {
        LOGGER.info("refresh replica slots ，old ones ：" + slotsReplicas.keySet() + "，new ones ：" + replicaSlotsList);

        for (String replicaSlots : replicaSlotsList) {
            if (!slotsReplicas.containsKey(replicaSlots)) {
                SlotsReplica slotsReplica = new SlotsReplica();
                slotsReplica.init(replicaSlots);
                slotsReplicas.put(replicaSlots, slotsReplica);
            }
        }

        byte[] bytes = JSONObject.toJSONString(replicaSlotsList).getBytes();
        FilePersistUtils.persist(bytes, NODE_SLOTS_REPLICAS_FILENAME);
    }

    /**
     * transfer slots
     *
     * @param targetNodeId
     * @param slots
     */
    public void transferSlots(Integer targetNodeId, String slots) {
        String[] slotsSplited = slots.split(",");
        Integer startSlotNo = Integer.valueOf(slotsSplited[0]);
        Integer endSlotNo = Integer.valueOf(slotsSplited[1]);

        for (int slotNo = startSlotNo; slotNo <= endSlotNo; slotNo++) {
            Slot slot = this.slots.getSlot(slotNo);
            if (slot.isEmpty()) {
                continue;
            }

            byte[] bytes = slot.getSlotData();

            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + bytes.length);
            buffer.putInt(MessageType.UPDATE_SLOTS);
            buffer.putInt(slotNo);
            buffer.put(bytes);

            ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
            serverNetworkManager.sendMessage(targetNodeId, buffer);

            this.slots.removeSlot(slotNo);
        }
    }

    /**
     * 将服务路由到slot
     *
     * @param serviceName
     * @return
     */
    private Integer routeSlot(String serviceName) {
        int hashCode = serviceName.hashCode() & Integer.MAX_VALUE;
        Integer slot = hashCode % SLOT_COUNT;

        if (slot == 0) {
            slot = slot + 1;
        }

        return slot;
    }

    public Integer getReplicaNodeId() {
        return slots.getReplicaNodeId();
    }

    public void updateSlotData(Integer slotNo, List<ServiceInstance> serviceInstances) {
        Slot slot = this.slots.getSlot(slotNo);
        slot.updateSlotData(serviceInstances);
    }

}
