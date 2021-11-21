package com.pantheon.server.slot.registry;

import java.util.Objects;


public class ServiceInstance {

    private String serviceName;
    private String serviceInstanceIp;
    private Integer serviceInstancePort;
    private volatile Long latestHeartbeatTime;

    public ServiceInstance(String serviceName, String serviceInstanceIp, Integer serviceInstancePort) {
        this.serviceName = serviceName;
        this.serviceInstanceIp = serviceInstanceIp;
        this.serviceInstancePort = serviceInstancePort;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getServiceInstanceIp() {
        return serviceInstanceIp;
    }

    public void setServiceInstanceIp(String serviceInstanceIp) {
        this.serviceInstanceIp = serviceInstanceIp;
    }

    public Integer getServiceInstancePort() {
        return serviceInstancePort;
    }

    public void setServiceInstancePort(Integer serviceInstancePort) {
        this.serviceInstancePort = serviceInstancePort;
    }

    public String getServiceInstanceId() {
        return serviceName + "_" + serviceInstanceIp + "_" + serviceInstancePort;
    }

    public Long getLatestHeartbeatTime() {
        return latestHeartbeatTime;
    }

    public void setLatestHeartbeatTime(Long latestHeartbeatTime) {
        this.latestHeartbeatTime = latestHeartbeatTime;
    }

    public String getAddress() {
        return serviceName + "," + serviceInstanceIp + "," + serviceInstancePort;
    }

    @Override
    public String toString() {
        return "ServiceInstance{" +
                "serviceName='" + serviceName + '\'' +
                ", serviceInstanceIp='" + serviceInstanceIp + '\'' +
                ", serviceInstancePort=" + serviceInstancePort +
                '}';
    }

    public static String getServiceInstanceId(String serviceName,
                                              String serviceInstanceIp,
                                              Integer serviceInstancePort) {
        return serviceName + "_" + serviceInstanceIp + "_" + serviceInstancePort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceInstance that = (ServiceInstance) o;
        return serviceName.equals(that.serviceName) &&
                serviceInstanceIp.equals(that.serviceInstanceIp) &&
                serviceInstancePort.equals(that.serviceInstancePort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, serviceInstanceIp, serviceInstancePort);
    }

}
