package com.pantheon.server.slot;


import com.pantheon.server.registry.InstanceRegistryImpl;
import com.pantheon.server.slot.registry.ServiceInstance;
import com.pantheon.server.slot.registry.ServiceRegistry;

import java.util.List;

/**
 * Slot play a role of routing and partitioning services data
 */
public class Slot {

    /**
     * slot num
     */
    private Integer slotNo;

    private InstanceRegistryImpl instanceRegistry;


    public Slot(Integer slotNo) {
        this.slotNo = slotNo;
    }



    public Slot(Integer slotNo,InstanceRegistryImpl instanceRegistry) {
        this.slotNo = slotNo;
        this.instanceRegistry =instanceRegistry;
    }

    public void setInstanceRegistry(InstanceRegistryImpl instanceRegistry) {
        this.instanceRegistry = instanceRegistry;
    }

    public void setSlotNo(Integer slotNo) {
        this.slotNo = slotNo;
    }

    public InstanceRegistryImpl getInstanceRegistry() {
        return instanceRegistry;
    }

    public Integer getSlotNo() {
        return slotNo;
    }
}
