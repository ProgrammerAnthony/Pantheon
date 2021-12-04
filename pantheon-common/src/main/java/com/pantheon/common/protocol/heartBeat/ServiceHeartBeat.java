
package com.pantheon.common.protocol.heartBeat;


import com.pantheon.remoting.protocol.RemotingSerializable;

public class ServiceHeartBeat<T> extends RemotingSerializable {
    private String appName;
    private String instanceId;
    private T instance;

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

    public T getInstance() {
        return instance;
    }

    public void setInstance(T instance) {
        this.instance = instance;
    }
}
