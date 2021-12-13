
package com.pantheon.common.protocol.heartBeat;


import com.pantheon.remoting.protocol.RemotingSerializable;

/**
 * locate a instance with a appName and instanceId
 */
public class ServiceUnregister extends RemotingSerializable {
    private String serviceName;
    private String instanceId;

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

}
