package com.pantheon.common.protocol.heartBeat;

/**
 * @author Anthony
 * @create 2021/12/13
 * @desc
 */
public class SubscriptionData {
    private Integer slotNum;
    private String serviceName;
    private String instanceId;
    private String clientId;

    public Integer getSlotNum() {
        return slotNum;
    }

    public void setSlotNum(Integer slotNum) {
        this.slotNum = slotNum;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
}
