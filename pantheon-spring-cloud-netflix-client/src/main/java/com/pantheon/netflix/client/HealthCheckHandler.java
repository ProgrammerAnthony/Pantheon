package com.pantheon.netflix.client;

import com.pantheon.client.appinfo.InstanceInfo;

/**
 * @author Anthony
 * @create 2021/12/17
 * @desc
 **/
public interface HealthCheckHandler {
    InstanceInfo.InstanceStatus getStatus(InstanceInfo.InstanceStatus var1);
}
