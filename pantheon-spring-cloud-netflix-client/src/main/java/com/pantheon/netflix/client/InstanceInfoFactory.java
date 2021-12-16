package com.pantheon.netflix.client;

import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.client.appinfo.LeaseInfo;
import com.pantheon.client.config.PantheonInstanceConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

/**
 * @author Anthony
 * @create 2021/12/16
 * @desc
 **/
public class InstanceInfoFactory {

    private static final Log log = LogFactory.getLog(InstanceInfoFactory.class);

    public InstanceInfo create(PantheonInstanceConfig config) {
        LeaseInfo.Builder leaseInfoBuilder = LeaseInfo.Builder.newBuilder()
                .setRenewalIntervalInSecs(config.getLeaseRenewalIntervalInSeconds())
                .setDurationInSecs(config.getLeaseExpirationDurationInSeconds());

        // Builder the instance information to be registered with eureka
        // server
        InstanceInfo.Builder builder = InstanceInfo.Builder.newBuilder();


        builder.setAppName(config.getServiceName())
                .setInstanceId(config.getInstanceId())
                .setIPAddr(config.getInstanceIpAddress()).setHostName(config.getInstanceHostName())
                .setPort(config.getInstancePort());

            InstanceInfo.InstanceStatus initialStatus = InstanceInfo.InstanceStatus.STARTING;
            if (log.isInfoEnabled()) {
                log.info("Setting initial instance status as: " + initialStatus);
            }
            builder.setStatus(initialStatus);

        // Add any user-specific metadata information
        for (Map.Entry<String, String> mapEntry : config.getMetadataMap().entrySet()) {
            String key = mapEntry.getKey();
            String value = mapEntry.getValue();
            builder.add(key, value);
        }

        InstanceInfo instanceInfo = builder.build();
        instanceInfo.setLeaseInfo(leaseInfoBuilder.build());
        return instanceInfo;
    }
}
