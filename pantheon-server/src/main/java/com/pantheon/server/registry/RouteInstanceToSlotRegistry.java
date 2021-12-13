package com.pantheon.server.registry;

import com.pantheon.client.appinfo.Application;
import com.pantheon.client.appinfo.Applications;
import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.server.slot.Slot;
import com.pantheon.server.slot.SlotManager;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Anthony
 * @create 2021/12/13
 * @desc service route to corresponding slot
 * todo singleton
 **/
public class RouteInstanceToSlotRegistry implements InstanceRegistry {
    public final SlotManager slotManager;
    public final ConcurrentHashMap<String/*serviceName*/, InstanceRegistryImpl> instanceRouteMap = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<Integer/*slot*/, String/*serviceName*/> slotServicesMap = new ConcurrentHashMap<>();

    public RouteInstanceToSlotRegistry() {
        slotManager = SlotManager.getInstance();
    }

    @Override
    public void register(InstanceInfo info) {
        InstanceRegistryImpl instanceRegistry = getInstanceRegistry(info.getAppName());
        instanceRegistry.register(info);
    }

    public synchronized InstanceRegistryImpl getInstanceRegistry(String instanceName) {
        InstanceRegistryImpl instanceRegistry = instanceRouteMap.get(instanceName);
        if (instanceRegistry == null) {
            Slot slot = slotManager.getSlot(instanceName);
            slotServicesMap.put(slot.getSlotNo(),instanceName);
            instanceRouteMap.put(instanceName, new InstanceRegistryImpl());
        }
        return instanceRegistry;
    }

    @Override
    public void register(InstanceInfo info, int leaseDuration) {
        InstanceRegistryImpl instanceRegistry = getInstanceRegistry(info.getAppName());
        instanceRegistry.register(info, leaseDuration);
    }

    @Override
    public Applications getApplications() {
        return null;
    }

    @Override
    public Applications getApplicationDeltas() {
        return null;
    }

    @Override
    public InstanceInfo getInstanceByAppAndId(String appName, String id) {
        InstanceRegistryImpl instanceRegistry = getInstanceRegistry(appName);
        return instanceRegistry.getInstanceByAppAndId(appName,id);
    }

    @Override
    public String renew(String appName, String id) {
        InstanceRegistryImpl instanceRegistry = getInstanceRegistry(appName);
        return instanceRegistry.renew(appName,id);
    }

    @Override
    public boolean statusUpdate(String appName, String id, InstanceInfo.InstanceStatus newStatus, String lastDirtyTimestamp) {
        InstanceRegistryImpl instanceRegistry = getInstanceRegistry(appName);
        return instanceRegistry.statusUpdate(appName, id, newStatus, lastDirtyTimestamp);
    }

    @Override
    public void storeOverriddenStatusIfRequired(String appName, String id, InstanceInfo.InstanceStatus overriddenStatus) {
        InstanceRegistryImpl instanceRegistry = getInstanceRegistry(appName);
        instanceRegistry.storeOverriddenStatusIfRequired(appName, id, overriddenStatus);
    }

    @Override
    public boolean deleteStatusOverride(String appName, String id, InstanceInfo.InstanceStatus newStatus, String lastDirtyTimestamp) {
        InstanceRegistryImpl instanceRegistry = getInstanceRegistry(appName);
       return instanceRegistry.deleteStatusOverride(appName, id, newStatus, lastDirtyTimestamp);
    }

    @Override
    public boolean cancel(String appName, String id) {
        InstanceRegistryImpl instanceRegistry = getInstanceRegistry(appName);
        return instanceRegistry.cancel(appName, id);
    }

    @Override
    public Application getApplication(String appName) {
        InstanceRegistryImpl instanceRegistry = getInstanceRegistry(appName);
        return instanceRegistry.getApplication(appName);
    }
}
