
package com.pantheon.common.protocol.heartBeat;


import com.pantheon.remoting.protocol.RemotingSerializable;

import java.util.HashSet;
import java.util.Set;

/**
 * locate a instance with a appName and instanceId
 */
public class HeartBeat extends RemotingSerializable {
    private String serviceName;
    private String instanceId;
    private Set<SubscriptionData> subscriptionDataSet = new HashSet<SubscriptionData>();

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public Set<SubscriptionData> getSubscriptionDataSet() {
        return subscriptionDataSet;
    }

    public void setSubscriptionDataSet(Set<SubscriptionData> subscriptionDataSet) {
        this.subscriptionDataSet = subscriptionDataSet;
    }
}
