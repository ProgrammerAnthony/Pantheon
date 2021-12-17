package com.pantheon.netflix.client;

import com.pantheon.client.appinfo.InstanceInfo;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.actuate.health.CompositeHealthIndicator;
import org.springframework.boot.actuate.health.HealthAggregator;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.cloud.client.discovery.health.DiscoveryCompositeHealthIndicator;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Anthony
 * @create 2021/12/17
 * @desc
 **/
public class PantheonHealthCheckHandler implements HealthCheckHandler, ApplicationContextAware, InitializingBean {
    private static final Map<Status, InstanceInfo.InstanceStatus> STATUS_MAPPING = new HashMap<Status, InstanceInfo.InstanceStatus>() {
        {
            this.put(Status.UNKNOWN, InstanceInfo.InstanceStatus.UNKNOWN);
            this.put(Status.OUT_OF_SERVICE, InstanceInfo.InstanceStatus.OUT_OF_SERVICE);
            this.put(Status.DOWN, InstanceInfo.InstanceStatus.DOWN);
            this.put(Status.UP, InstanceInfo.InstanceStatus.UP);
        }
    };
    private final CompositeHealthIndicator healthIndicator;
    private ApplicationContext applicationContext;

    public PantheonHealthCheckHandler(HealthAggregator healthAggregator) {
        Assert.notNull(healthAggregator, "HealthAggregator must not be null");
        this.healthIndicator = new CompositeHealthIndicator(healthAggregator);
    }

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public void afterPropertiesSet() throws Exception {
        Map<String, HealthIndicator> healthIndicators = this.applicationContext.getBeansOfType(HealthIndicator.class);
        Iterator var2 = healthIndicators.entrySet().iterator();

        while(true) {
            while(var2.hasNext()) {
                Map.Entry<String, HealthIndicator> entry = (Map.Entry)var2.next();
                if (entry.getValue() instanceof DiscoveryCompositeHealthIndicator) {
                    DiscoveryCompositeHealthIndicator indicator = (DiscoveryCompositeHealthIndicator)entry.getValue();
                    Iterator var5 = indicator.getHealthIndicators().iterator();

                    while(var5.hasNext()) {
                        DiscoveryCompositeHealthIndicator.Holder holder = (DiscoveryCompositeHealthIndicator.Holder)var5.next();
                        if (!(holder.getDelegate() instanceof PantheonHealthIndicator)) {
                            this.healthIndicator.addHealthIndicator(holder.getDelegate().getName(), holder);
                        }
                    }
                } else {
                    this.healthIndicator.addHealthIndicator((String)entry.getKey(), (HealthIndicator)entry.getValue());
                }
            }

            return;
        }
    }

    public InstanceInfo.InstanceStatus getStatus(InstanceInfo.InstanceStatus instanceStatus) {
        return this.getHealthStatus();
    }

    protected InstanceInfo.InstanceStatus getHealthStatus() {
        Status status = this.healthIndicator.health().getStatus();
        return this.mapToInstanceStatus(status);
    }

    protected InstanceInfo.InstanceStatus mapToInstanceStatus(Status status) {
        return !STATUS_MAPPING.containsKey(status) ? InstanceInfo.InstanceStatus.UNKNOWN : (InstanceInfo.InstanceStatus)STATUS_MAPPING.get(status);
    }
}
