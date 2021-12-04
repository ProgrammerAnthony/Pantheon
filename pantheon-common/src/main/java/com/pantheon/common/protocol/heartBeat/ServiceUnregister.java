
package com.pantheon.common.protocol.heartBeat;


import com.pantheon.remoting.protocol.RemotingSerializable;

public class ServiceUnregister extends RemotingSerializable {
    private String appName;
    private String instanceId;

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }
}
