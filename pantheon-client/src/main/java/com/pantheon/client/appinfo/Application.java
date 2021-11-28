package com.pantheon.client.appinfo;

import com.pantheon.client.config.DefaultInstanceConfig;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Anthony
 * @create 2021/11/28
 * @desc
 * The Application class holds the list of instances for a particular
 * application.
 */
public class Application {

    @Override
    public String toString() {
        return "Application [name=" + name + ", isDirty=" + isDirty
                + ", instances=" + instances + ", instancesMap=" + instancesMap + "]";
    }



    private String name;

    private volatile boolean isDirty = false;

    private AtomicReference<List<InstanceInfo>> shuffledInstances = new AtomicReference<List<InstanceInfo>>();


    private final Set<InstanceInfo> instances;

    private Map<String, InstanceInfo> instancesMap;

    public Application() {
        instances = new LinkedHashSet<InstanceInfo>();
        instancesMap = new ConcurrentHashMap<String, InstanceInfo>();
    }

    public Application(String name) {
        this.name = name;
        instancesMap = new ConcurrentHashMap<String, InstanceInfo>();
        instances = new LinkedHashSet<InstanceInfo>();
    }

    public Application(String name,
                       List<InstanceInfo> instances) {
        this(name);
        for (InstanceInfo instanceInfo : instances) {
            addInstance(instanceInfo);
        }
    }

    /**
     * Add the given instance info the list.
     *
     * @param i the instance info object to be added.
     */
    public void addInstance(InstanceInfo i) {
        instancesMap.put(i.getId(), i);
        synchronized (instances) {
            instances.remove(i);
            instances.add(i);
            isDirty = true;
        }
    }

    /**
     * Remove the given instance info the list.
     *
     * @param i the instance info object to be removed.
     */
    public void removeInstance(InstanceInfo i) {
        removeInstance(i, true);
    }


    /**
     * Get the instance info that matches the given id.
     *
     * @param id the id for which the instance info needs to be returned.
     * @return the instance info object.
     */
    public InstanceInfo getByInstanceId(String id) {
        return instancesMap.get(id);
    }

    /**
     * Gets the name of the application.
     *
     * @return the name of the application.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the application.
     *
     * @param name the name of the application.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the number of instances in this application
     */
    public int size() {
        return instances.size();
    }


    /**
     * Gets the list of non-shuffled and non-filtered instances associated with this particular
     * application.
     *
     * @return list of non-shuffled and non-filtered instances associated with this particular
     *         application.
     */
    public List<InstanceInfo> getInstancesAsIsFromPantheon() {
        synchronized (instances) {
            return new ArrayList<InstanceInfo>(this.instances);
        }
    }
    /**
     * Gets the list of instances associated with this particular application.
     * <p>
     * Note that the instances are always returned with random order after
     * shuffling to avoid traffic to the same instances during startup. The
     * shuffling always happens once after every fetch cycle as specified in
     * {@link DefaultInstanceConfig#getRegistryFetchIntervalSeconds()}.
     * </p>
     *
     * @return the list of shuffled instances associated with this application.
     */
    public List<InstanceInfo> getInstances() {
        if (this.shuffledInstances.get() == null) {
            return this.getInstancesAsIsFromPantheon();
        } else {
            return this.shuffledInstances.get();
        }
    }


    /**
     * Shuffles the list of instances in the application and stores it for
     * future retrievals.
     *
     * @param filterUpInstances
     *            indicates whether only the instances with status
     *            {@link InstanceInfo.InstanceStatus#UP} needs to be stored.
     */
    public void shuffleAndStoreInstances(boolean filterUpInstances) {
        _shuffleAndStoreInstances(filterUpInstances);
    }

    private void _shuffleAndStoreInstances(boolean filterUpInstances) {
        List<InstanceInfo> instanceInfoList;
        synchronized (instances) {
            instanceInfoList = new ArrayList<InstanceInfo>(instances);
        }
        if (filterUpInstances) {
            Iterator<InstanceInfo> it = instanceInfoList.iterator();
            while (it.hasNext()) {
                InstanceInfo instanceInfo = it.next();
                if (filterUpInstances && !InstanceInfo.InstanceStatus.UP.equals(instanceInfo.getStatus())) {
                    it.remove();
                }
            }

        }
        Collections.shuffle(instanceInfoList);
        this.shuffledInstances.set(instanceInfoList);
    }

    private void removeInstance(InstanceInfo i, boolean markAsDirty) {
        instancesMap.remove(i.getId());
        synchronized (instances) {
            instances.remove(i);
            if (markAsDirty) {
                isDirty = true;
            }
        }
    }
}
