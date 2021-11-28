package com.pantheon.client.appinfo;

import com.pantheon.client.config.PantheonInstanceConfig;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Anthony
 * @create 2021/11/28
 * @desc The application class holds the list of instances for a particular
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
     * Gets the list of instances associated with this particular application.
     *
     * @return the list of shuffled instances associated with this application.
     */
    public List<InstanceInfo> getInstances() {
        return null;
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
