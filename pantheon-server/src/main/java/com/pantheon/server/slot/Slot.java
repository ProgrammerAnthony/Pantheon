package com.pantheon.server.slot;


import com.pantheon.server.registry.InstanceRegistryImpl;
import com.pantheon.server.slot.registry.ServiceInstance;
import com.pantheon.server.slot.registry.ServiceRegistry;

import java.util.List;

/**
 * Slot in Pantheon is topic in RocketMq, Slot is the authentic part for
 * service registry to save .
 * todo build with new mechanism
 */
public class Slot {

    /**
     * slot num
     */
    private Integer slotNo;

    private InstanceRegistryImpl instanceRegistry;

    public Slot(Integer slotNo,InstanceRegistryImpl instanceRegistry) {
        this.slotNo = slotNo;
        this.instanceRegistry =instanceRegistry;
    }

    public ServiceRegistry getServiceRegistry() {
//        return this.serviceRegistry;
        return null;
    }

    public boolean isEmpty() {
        return  false;
//        return serviceRegistry.isEmpty();
    }

    public byte[] getSlotData() {
        return null;
//        return serviceRegistry.getData();
    }

    public void updateSlotData(List<ServiceInstance> serviceInstances) {
//        serviceRegistry.updateData(serviceInstances);
    }

    public void setInstanceRegistry(InstanceRegistryImpl instanceRegistry) {
        this.instanceRegistry = instanceRegistry;
    }

    public InstanceRegistryImpl getInstanceRegistry() {
        return instanceRegistry;
    }

    public Integer getSlotNo() {
        return slotNo;
    }
}
