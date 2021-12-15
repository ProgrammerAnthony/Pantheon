package com.pantheon.server.slot;

import com.pantheon.server.slot.registry.ServiceRegistry;

import java.util.concurrent.ConcurrentHashMap;


/**
 * slots replica
 *
 * 1 todo build with new register mechanism
 * 2 todo load specific instances not all
 */
public class SlotsReplica {

    private ConcurrentHashMap<Integer/*slotNo*/, Slot> slots =
            new ConcurrentHashMap<>();

    public void init(String slotScope) {
        String[] slotScopeSplited = slotScope.split(",");
        Integer startSlotNo = Integer.valueOf(slotScopeSplited[0]);
        Integer endSlotNo = Integer.valueOf(slotScopeSplited[1]);
        for(Integer slotNo = startSlotNo; slotNo <= endSlotNo; slotNo++) {
            slots.put(slotNo, new Slot(slotNo));
        }
    }

    public Slot getSlot(Integer slotNo) {
        return slots.get(slotNo);
    }

    public ConcurrentHashMap<Integer, Slot> getSlots() {
        return slots;
    }
}
