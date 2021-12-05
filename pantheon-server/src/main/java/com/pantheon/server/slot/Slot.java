package com.pantheon.server.slot;


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
    /**
     * service registry
     */
    private ServiceRegistry serviceRegistry;


    public Slot(Integer slotNo, ServiceRegistry serviceRegistry) {
        this.slotNo = slotNo;
        this.serviceRegistry = serviceRegistry;
    }

    public ServiceRegistry getServiceRegistry() {
        return this.serviceRegistry;
    }

    public boolean isEmpty() {
        return serviceRegistry.isEmpty();
    }

    public byte[] getSlotData() {
        return serviceRegistry.getData();
    }

    public void updateSlotData(List<ServiceInstance> serviceInstances) {
        serviceRegistry.updateData(serviceInstances);
    }

}
