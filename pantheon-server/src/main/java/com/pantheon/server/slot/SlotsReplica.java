package com.pantheon.server.slot;

import com.pantheon.server.slot.registry.ServiceRegistry;

import java.util.concurrent.ConcurrentHashMap;


/**
 * slots replica
 */
public class SlotsReplica {

    private ConcurrentHashMap<Integer/*slotNo*/, Slot> slots =
            new ConcurrentHashMap<>();

    public void init(String slotScope) {
        String[] slotScopeSplited = slotScope.split(",");
        Integer startSlotNo = Integer.valueOf(slotScopeSplited[0]);
        Integer endSlotNo = Integer.valueOf(slotScopeSplited[1]);

        ServiceRegistry serviceRegistry = new ServiceRegistry(true);

        for(Integer slotNo = startSlotNo; slotNo <= endSlotNo; slotNo++) {
            slots.put(slotNo, new Slot(slotNo, serviceRegistry));
        }
    }

    public Slot getSlot(Integer slotNo) {
        return slots.get(slotNo);
    }

    public ConcurrentHashMap<Integer, Slot> getSlots() {
        return slots;
    }
}
