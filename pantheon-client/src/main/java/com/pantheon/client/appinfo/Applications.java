package com.pantheon.client.appinfo;

import com.pantheon.client.config.DefaultInstanceConfig;
import com.pantheon.remoting.protocol.RemotingSerializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Anthony
 * @create 2021/11/28
 * @desc The class that wraps all the registry information returned by Pantheon server.
 *
 * <p>
 * Note that the registry information is fetched from pantheon server as specified
 * in {@link DefaultInstanceConfig#getRegistryFetchIntervalSeconds()}.  Once the
 * information is fetched it is shuffled and also filtered for instances with
 * {@link InstanceInfo.InstanceStatus#UP} status as specified by the configuration
 * {@link DefaultInstanceConfig#shouldFilterOnlyUpInstances()}.
 * </p>
 */
public class Applications extends RemotingSerializable {
    private static final String APP_INSTANCEID_DELIMITER = "$$";
    private static final Logger logger = LoggerFactory.getLogger(Applications.class);
    private static final String STATUS_DELIMITER = "_";

    private Long versionDelta = Long.valueOf(-1);


    private AbstractQueue<Application> applications;

    private Map<String/*appName*/, Application> appNameApplicationMap = new ConcurrentHashMap<String, Application>();

    private Map<String/*appName*/, AtomicReference<List<InstanceInfo>>> shuffleVirtualHostNameMap = new ConcurrentHashMap<String, AtomicReference<List<InstanceInfo>>>();
    private Map<String/*appName*/, AtomicReference<List<InstanceInfo>>> shuffledSecureVirtualHostNameMap = new ConcurrentHashMap<String, AtomicReference<List<InstanceInfo>>>();

    private String appsHashCode;

    /**
     * Create a new, empty Pantheon application list.
     */
    public Applications() {
        this.applications = new ConcurrentLinkedQueue<>();
    }

    /**
     * Note that appsHashCode and versionDelta key names are formatted in a custom/configurable way.
     */
    public Applications(
            String appsHashCode,
            Long versionDelta,
            List<Application> registeredApplications) {
        this.applications = new ConcurrentLinkedQueue<>();
        for (Application app : registeredApplications) {
            this.addApplication(app);
        }
        this.appsHashCode = appsHashCode;
        this.versionDelta = versionDelta;
    }

    /**
     * Create a new Pantheon application list, based on the provided applications.  The provided container is
     * not modified.
     *
     * @param apps the initial list of apps to store in this applications list
     */
    public Applications(List<Application> apps) {
        this.applications = new ConcurrentLinkedQueue<Application>();
        this.applications.addAll(apps);
    }

    /**
     * Add the <em>application</em> to the list.
     *
     * @param app the <em>application</em> to be added.
     */
    public void addApplication(Application app) {
        appNameApplicationMap.put(app.getName().toUpperCase(Locale.ROOT), app);
        applications.add(app);
    }


    /**
     * Gets the list of all registered <em>applications</em> from pantheon.
     *
     * @return list containing all applications registered with pantheon.
     */
    public List<Application> getRegisteredApplications() {
        List<Application> list = new ArrayList<Application>();
        list.addAll(this.applications);
        return list;
    }

    /**
     * Gets the list of all registered <em>applications</em> for the given
     * application name.
     *
     * @param appName the application name for which the result need to be fetched.
     * @return the list of registered applications for the given application
     * name.
     */
    public Application getRegisteredApplications(String appName) {
        return appNameApplicationMap.get(appName.toUpperCase(Locale.ROOT));
    }


    /**
     * @return a weakly consistent size of the number of instances in all the applications
     */
    public int size() {
        int result = 0;
        for (Application application : applications) {
            result += application.size();
        }

        return result;
    }


    /**
     * Used by the pantheon server. Not for external use.
     *
     * @param hashCode the hash code to assign for this app collection
     */
    public void setAppsHashCode(String hashCode) {
        this.appsHashCode = hashCode;
    }

    /**
     * Used by the pantheon server. Not for external use.
     *
     * @return the string indicating the hashcode based on the applications stored.
     */
    public String getAppsHashCode() {
        return this.appsHashCode;
    }

    /**
     * Gets the hash code for this <em>applications</em> instance. Used for
     * comparison of instances between pantheon server and pantheon client.
     *
     * @return the internal hash code representation indicating the information
     * about the instances.
     */
    public String getReconcileHashCode() {
        TreeMap<String, AtomicInteger> instanceCountMap = new TreeMap<String, AtomicInteger>();
        populateInstanceCountMap(instanceCountMap);
        return getReconcileHashCode(instanceCountMap);
    }

    /**
     * Populates the provided instance count map.  The instance count map is used as part of the general
     * app list synchronization mechanism.
     *
     * @param instanceCountMap the map to populate
     */
    public void populateInstanceCountMap(TreeMap<String, AtomicInteger> instanceCountMap) {
        for (Application app : this.getRegisteredApplications()) {
            for (InstanceInfo info : app.getInstancesAsIsFromPantheon()) {
                AtomicInteger instanceCount = instanceCountMap.get(info.getStatus().name());
                if (instanceCount == null) {
                    instanceCount = new AtomicInteger(0);
                    instanceCountMap.put(info.getStatus().name(), instanceCount);
                }
                instanceCount.incrementAndGet();
            }
        }
    }

    /**
     * Gets the reconciliation hashcode.  The hashcode is used to determine whether the applications list
     * has changed since the last time it was acquired.
     *
     * @param instanceCountMap the instance count map to use for generating the hash
     * @return the hash code for this instance
     */
    public static String getReconcileHashCode(TreeMap<String, AtomicInteger> instanceCountMap) {
        String reconcileHashCode = "";
        for (Map.Entry<String, AtomicInteger> mapEntry : instanceCountMap.entrySet()) {
            reconcileHashCode = reconcileHashCode + mapEntry.getKey()
                    + STATUS_DELIMITER + mapEntry.getValue().get()
                    + STATUS_DELIMITER;
        }
        return reconcileHashCode;
    }

    /**
     * Gets the exact difference between this applications instance and another
     * one.
     *
     * @param apps the applications for which to compare this one.
     * @return a map containing the differences between the two.
     */
    public Map<String, List<String>> getReconcileMapDiff(Applications apps) {
        Map<String, List<String>> diffMap = new TreeMap<String, List<String>>();
        Set<Pair> allInstanceAppInstanceIds = new HashSet<Pair>();
        for (Application otherApp : apps.getRegisteredApplications()) {
            Application thisApp = this.getRegisteredApplications(otherApp.getName());
            if (thisApp == null) {
                logger.warn("Application not found in local cache : {}", otherApp.getName());
                continue;
            }
            for (InstanceInfo instanceInfo : thisApp.getInstancesAsIsFromPantheon()) {
                allInstanceAppInstanceIds.add(new Pair(thisApp.getName(),
                        instanceInfo.getId()));
            }
            for (InstanceInfo otherInstanceInfo : otherApp.getInstancesAsIsFromPantheon()) {
                InstanceInfo thisInstanceInfo = thisApp.getByInstanceId(otherInstanceInfo.getId());
                if (thisInstanceInfo == null) {
                    List<String> diffList = diffMap.get(InstanceInfo.ActionType.DELETED.name());
                    if (diffList == null) {
                        diffList = new ArrayList<String>();
                        diffMap.put(InstanceInfo.ActionType.DELETED.name(), diffList);
                    }
                    diffList.add(otherInstanceInfo.getId());
                } else if (!thisInstanceInfo.getStatus().name()
                        .equalsIgnoreCase(otherInstanceInfo.getStatus().name())) {
                    List<String> diffList = diffMap.get(InstanceInfo.ActionType.MODIFIED.name());
                    if (diffList == null) {
                        diffList = new ArrayList<String>();
                        diffMap.put(InstanceInfo.ActionType.MODIFIED.name(), diffList);
                    }
                    diffList.add(thisInstanceInfo.getId()
                            + APP_INSTANCEID_DELIMITER
                            + thisInstanceInfo.getStatus().name()
                            + APP_INSTANCEID_DELIMITER
                            + otherInstanceInfo.getStatus().name());
                }
                allInstanceAppInstanceIds.remove(new Pair(otherApp.getName(), otherInstanceInfo.getId()));
            }
        }
        for (Pair pair : allInstanceAppInstanceIds) {
            Application app = new Application(pair.getItem1());
            InstanceInfo thisInstanceInfo = app.getByInstanceId(pair.getItem2());
            if (thisInstanceInfo != null) {
                List<String> diffList = diffMap.get(InstanceInfo.ActionType.ADDED.name());
                if (diffList == null) {
                    diffList = new ArrayList<String>();
                    diffMap.put(InstanceInfo.ActionType.ADDED.name(), diffList);
                }
                diffList.add(thisInstanceInfo.getId());
            }
        }
        return diffMap;

    }

    private static final class Pair {
        private final String item1;
        private final String item2;

        public Pair(String item1, String item2) {
            super();
            this.item1 = item1;
            this.item2 = item2;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((item1 == null) ? 0 : item1.hashCode());
            result = prime * result
                    + ((item2 == null) ? 0 : item2.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Pair other = (Pair) obj;
            if (item1 == null) {
                if (other.item1 != null) {
                    return false;
                }
            } else if (!item1.equals(other.item1)) {
                return false;
            }
            if (item2 == null) {
                if (other.item2 != null) {
                    return false;
                }
            } else if (!item2.equals(other.item2)) {
                return false;
            }
            return true;
        }

        public String getItem1() {
            return item1;
        }

        public String getItem2() {
            return item2;
        }
    }

    /**
     * Shuffles the provided instances so that they will not always be returned in the same order.
     *
     * @param filterUpInstances whether to return only UP instances
     */
    public void shuffleInstances(boolean filterUpInstances) {
        for (Application application : appNameApplicationMap.values()) {
            application.shuffleAndStoreInstances(filterUpInstances);
        }
    }


}
