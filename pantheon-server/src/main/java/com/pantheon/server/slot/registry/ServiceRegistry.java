package com.pantheon.server.slot.registry;

import com.alibaba.fastjson.JSONObject;
import com.pantheon.server.config.ArchaiusPantheonServerConfig;
import com.pantheon.server.config.CachedPantheonServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


public class ServiceRegistry {

    boolean isReplica;

    public ServiceRegistry(boolean isReplica) {
        this.isReplica = isReplica;
        new HeartbeatCheckThread().start();
    }

    /**
     * cache service registry
     */
    private ConcurrentHashMap<String/*serviceInstanceId*/, List<ServiceInstance>> serviceRegistryData =
            new ConcurrentHashMap<>();
    private ConcurrentHashMap<String/*serviceInstanceId*/, ServiceInstance> serviceInstanceData =
            new ConcurrentHashMap<>();
    private ConcurrentHashMap<String/*serviceInstanceId*/, List<ServiceChangedListener>> serviceChangedListenerData =
            new ConcurrentHashMap<>();

    public void updateData(List<ServiceInstance> serviceInstances) {
        for (ServiceInstance serviceInstance : serviceInstances) {
            String serviceName = serviceInstance.getServiceName();

            if (serviceRegistryData.get(serviceName) == null) {
                serviceRegistryData.put(serviceName, new ArrayList<ServiceInstance>());
            }
            serviceRegistryData.get(serviceName).add(serviceInstance);

            serviceInstanceData.put(serviceInstance.getServiceInstanceId(), serviceInstance);
        }
    }

    public boolean isEmpty() {
        return serviceRegistryData.isEmpty();
    }

    public byte[] getData() {
        List<ServiceInstance> allServiceInstances = new ArrayList<ServiceInstance>();
        for (List<ServiceInstance> serviceInstances : serviceRegistryData.values()) {
            allServiceInstances.addAll(serviceInstances);
        }
        return JSONObject.toJSONString(allServiceInstances).getBytes();
    }

    /**
     * register service instance
     *
     * @param serviceInstance
     */
    public void register(ServiceInstance serviceInstance) {
        // add this instance into registry
        List<ServiceInstance> serviceInstances = serviceRegistryData.get(
                serviceInstance.getServiceName());
        if (serviceInstances == null) {
            synchronized (this) {
                if (serviceInstances == null) {
                    serviceInstances = new CopyOnWriteArrayList<>();
                    serviceRegistryData.put(serviceInstance.getServiceName(), serviceInstances);
                }
            }
        }
        serviceInstances.add(serviceInstance);

        serviceInstanceData.put(serviceInstance.getServiceInstanceId(),
                serviceInstance);

        // notify service changed

        if (!isReplica) {
            List<ServiceChangedListener> serviceChangedListeners =
                    serviceChangedListenerData.get(serviceInstance.getServiceName());

            if (serviceChangedListeners == null) {
                synchronized (this) {
                    if (serviceChangedListeners == null) {
                        serviceChangedListeners = new CopyOnWriteArrayList<>();
                        serviceChangedListenerData.put(serviceInstance.getServiceName(), serviceChangedListeners);
                    }
                }
            }

            for (ServiceChangedListener serviceChangedListener : serviceChangedListeners) {
                serviceChangedListener.onChange(serviceInstance.getServiceName(),
                        serviceRegistryData.get(serviceInstance.getServiceName()));
            }
        }
    }

    /**
     * service instance heartbeat
     *
     * @param serviceName
     * @param serviceInstanceIp
     * @param serviceInstancePort
     */
    public void heartbeat(String serviceName,
                          String serviceInstanceIp,
                          Integer serviceInstancePort) {
        String serviceInstanceId = ServiceInstance.getServiceInstanceId(
                serviceName, serviceInstanceIp, serviceInstancePort
        );
        ServiceInstance serviceInstance = serviceInstanceData.get(serviceInstanceId);

        if (serviceInstance == null) {
            synchronized (this) {
                if (serviceInstance == null) {
                    serviceInstance = new ServiceInstance(
                            serviceName, serviceInstanceIp, serviceInstancePort);
                    serviceInstanceData.put(serviceInstanceId, serviceInstance);

                    List<ServiceInstance> serviceInstances = serviceRegistryData.get(serviceName);
                    if (serviceInstances == null) {
                        serviceRegistryData.put(serviceName, new CopyOnWriteArrayList<>());
                    }
                    serviceInstances.add(serviceInstance);
                }
            }
        }

        serviceInstance.setLatestHeartbeatTime(new Date().getTime());
        System.out.println("receive heart beat from " + serviceInstanceId + "......");
    }

    /**
     * service registry/subscription
     *
     * @param serviceName
     * @return
     */
    public List<ServiceInstance> subscribe(String clientConnectionId, String serviceName) {
        List<ServiceChangedListener> serviceChangedListeners =
                serviceChangedListenerData.get(serviceName);

        if (serviceChangedListeners == null) {
            synchronized (this) {
                if (serviceChangedListeners == null) {
                    serviceChangedListeners = new CopyOnWriteArrayList<>();
                    serviceChangedListenerData.put(serviceName, serviceChangedListeners);
                }
            }
        }

        serviceChangedListeners.add(new ServiceChangedListener(clientConnectionId));

        return serviceRegistryData.get(serviceName);
    }

    /**
     * heartbeat check
     */
    class HeartbeatCheckThread extends Thread {

        private final Logger LOGGER = LoggerFactory.getLogger(HeartbeatCheckThread.class);

        @Override
        public void run() {
            ArchaiusPantheonServerConfig config = CachedPantheonServerConfig.getInstance();

            Integer heartbeatCheckInterval = config.getHeartBeatCheckInterval();
            Integer heartbeatTimeoutPeriod = config.getHeartbeatTimeoutPeriod();

            while (true) {
                long now = new Date().getTime();

                List<String> removeServiceInstanceIds = new ArrayList<String>();
                Set<String> changedServiceNames = new HashSet<String>();

                for (ServiceInstance serviceInstance : serviceInstanceData.values()) {
                    if (now - serviceInstance.getLatestHeartbeatTime() > heartbeatTimeoutPeriod * 1000L) {
                        List<ServiceInstance> serviceInstances =
                                serviceRegistryData.get(serviceInstance.getServiceName());
                        serviceInstances.remove(serviceInstance);
                        removeServiceInstanceIds.add(serviceInstance.getServiceInstanceId());
                        LOGGER.info("service instance {} get removed after 5s without heartbeat" ,serviceInstance);

                        changedServiceNames.add(serviceInstance.getServiceName());
                    }
                }

                for (String serviceInstanceId : removeServiceInstanceIds) {
                    serviceInstanceData.remove(serviceInstanceId);
                }

                // notify service changed
                if (!isReplica) {
                    for (String serviceName : changedServiceNames) {
                        List<ServiceChangedListener> serviceChangedListeners =
                                serviceChangedListenerData.get(serviceName);
                        for (ServiceChangedListener serviceChangedListener : serviceChangedListeners) {
                            serviceChangedListener.onChange(serviceName, serviceRegistryData.get(serviceName));
                        }
                    }
                }

                removeServiceInstanceIds.clear();
                changedServiceNames.clear();

                try {
                    Thread.sleep(heartbeatCheckInterval * 1000L);
                } catch (InterruptedException e) {
                    LOGGER.error("heartbeat check with InterruptedException ！！！", e);
                }
            }
        }
    }

}
