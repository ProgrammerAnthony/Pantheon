package com.pantheon.server.slot;


import com.pantheon.server.slot.registry.ServiceRegistry;

import java.util.concurrent.ConcurrentHashMap;

/**
 * slots in current node
 *  todo build with new register mechanism
 */
public class Slots {

    /**
     * slots in memory
     */
    private ConcurrentHashMap<Integer/*slotNo*/, Slot> slots =
            new ConcurrentHashMap<>();
    /**
     * corresponding replica slots id
     */
    private Integer replicaNodeId;

    /**
     * slots initialization
     * @param slotScope
     */
    public void init(String slotScope) {
        String[] slotScopeSplited = slotScope.split(",");
        Integer startSlotNo = Integer.valueOf(slotScopeSplited[0]);
        Integer endSlotNo = Integer.valueOf(slotScopeSplited[1]);

        ServiceRegistry serviceRegistry = new ServiceRegistry(false);

        for(Integer slotNo = startSlotNo; slotNo <= endSlotNo; slotNo++) {
            slots.put(slotNo, new Slot(slotNo, serviceRegistry));
        }
    }

    public void putSlot(Integer slotNo, Slot slot) {
        slots.put(slotNo, slot);
    }

    /**
     * corresponding replica slots id
     * @param replicaNodeId
     */
    public void setReplicaNodeId(Integer replicaNodeId) {
        this.replicaNodeId = replicaNodeId;
    }

    public Slot getSlot(Integer slotNo) {
        return slots.get(slotNo);
    }

    public void removeSlot(Integer slotNo) {
        slots.remove(slotNo);
    }

    public Integer getReplicaNodeId() {
        return replicaNodeId;
    }

}
