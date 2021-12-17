package com.pantheon.netflix.client;

import com.pantheon.client.appinfo.InstanceInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.client.serviceregistry.ServiceRegistry;

import java.util.HashMap;

/**
 * @author Anthony
 * @create 2021/12/17
 * @desc
 **/
public class PantheonServiceRegistry implements ServiceRegistry<PantheonRegistration> {
    private static final Log log = LogFactory.getLog(PantheonServiceRegistry.class);

    public PantheonServiceRegistry() {
    }

    public void register(PantheonRegistration reg) {
        this.maybeInitializeClient(reg);
        if (log.isInfoEnabled()) {
//            log.info("Registering application " + reg.getInstanceConfig().getServiceName() + " with eureka with status " + reg.getInstanceConfig().getInitialStatus());
        }

//todo        reg.getApplicationInfoManager().setInstanceStatus(reg.getInstanceConfig().getInitialStatus());
        if (reg.getHealthCheckHandler() != null) {
//todo            reg.getEurekaClient().registerHealthCheck(reg.getHealthCheckHandler());
        }

    }

    private void maybeInitializeClient(PantheonRegistration reg) {
//        reg.getApplicationInfoManager().getInfo();
        reg.getEurekaClient().getApplications();
    }

    public void deregister(PantheonRegistration reg) {
//        if (reg.getApplicationInfoManager().getInfo() != null) {
//            if (log.isInfoEnabled()) {
//                log.info("Unregistering application " + reg.getInstanceConfig().getAppname() + " with eureka with status DOWN");
//            }
//
//            reg.getApplicationInfoManager().setInstanceStatus(InstanceStatus.DOWN);
//        }

    }

    public void setStatus(PantheonRegistration registration, String status) {
//        InstanceInfo info = registration.getApplicationInfoManager().getInfo();
        if ("CANCEL_OVERRIDE".equalsIgnoreCase(status)) {
//            registration.getEurekaClient().cancelOverrideStatus(info);
        } else {
            InstanceInfo.InstanceStatus newStatus = InstanceInfo.InstanceStatus.toEnum(status);
//            registration.getEurekaClient().setStatus(newStatus, info);
        }
    }

    public Object getStatus(PantheonRegistration registration) {
        HashMap<String, Object> status = new HashMap();
//        InstanceInfo info = registration.getApplicationInfoManager().getInfo();
        InstanceInfo info=new InstanceInfo();
        status.put("status", info.getStatus().toString());
        status.put("overriddenStatus", info.getOverriddenStatus().toString());
        return status;
    }

    public void close() {
    }
}
