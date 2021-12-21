package com.pantheon.netflix.client;

import com.pantheon.client.DiscoveryClientNode;
import com.pantheon.client.appinfo.Application;
import com.pantheon.client.appinfo.Applications;
import com.pantheon.client.config.PantheonInstanceConfig;
import com.pantheon.client.discovery.DiscoveryClient;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.cloud.client.discovery.health.DiscoveryHealthIndicator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Anthony
 * @create 2021/12/16
 * @desc
 **/
public class PantheonHealthIndicator implements DiscoveryHealthIndicator {

    private final DiscoveryClientNode pantheonClient;

    private final PantheonInstanceConfig instanceConfig;

    public PantheonHealthIndicator(DiscoveryClientNode pantheonClient, PantheonInstanceConfig instanceConfig) {
        this.pantheonClient = pantheonClient;
        this.instanceConfig = instanceConfig;
    }

    @Override
    public String getName() {
        return "pantheon";
    }

    @Override
    public Health health() {
        Health.Builder builder = Health.unknown();
        Status status = getStatus(builder);
        return builder.status(status).withDetail("applications", getApplications())
                .build();
    }

    private Status getStatus(Health.Builder builder) {
        Status status = new Status(
                this.pantheonClient.getInstanceRemoteStatus().toString(),
                "Remote status from Pantheon server");

        if (pantheonClient instanceof DiscoveryClientNode && instanceConfig.shouldFetchRegistry()) {
            DiscoveryClientNode discoveryClient = (DiscoveryClientNode) pantheonClient;
            long lastFetch = discoveryClient.getLastSuccessfulRegistryFetchTimePeriod();

            if (lastFetch < 0) {
                status = new Status("UP",
                        "Pantheon discovery client has not yet successfully connected to a Pantheon server");
            }
            else if (lastFetch > instanceConfig.getRegistryFetchIntervalSeconds() * 2000) {
                status = new Status("UP",
                        "Pantheon discovery client is reporting failures to connect to a Pantheon server");
                builder.withDetail("renewalPeriod",
                        instanceConfig.getLeaseRenewalIntervalInSeconds());
                builder.withDetail("failCount",
                        lastFetch / instanceConfig.getRegistryFetchIntervalSeconds());
            }
        }

        return status;
    }

    private Map<String, Object> getApplications() {
        Applications applications = this.pantheonClient.getApplications();
        if (applications == null) {
            return Collections.emptyMap();
        }
        Map<String, Object> result = new HashMap<>();
        for (Application application : applications.getRegisteredApplications()) {
            result.put(application.getName(), application.getInstances().size());
        }
        return result;
    }

}