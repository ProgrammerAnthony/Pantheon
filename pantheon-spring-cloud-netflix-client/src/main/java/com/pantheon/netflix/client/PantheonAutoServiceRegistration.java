package com.pantheon.netflix.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.context.embedded.EmbeddedServletContainerInitializedEvent;
import org.springframework.cloud.client.discovery.event.InstanceRegisteredEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.Ordered;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Anthony
 * @create 2021/12/17
 * @desc
 **/
public class PantheonAutoServiceRegistration  implements AutoServiceRegistration, SmartLifecycle, Ordered {
    private static final Log log = LogFactory.getLog(PantheonAutoServiceRegistration.class);
    private AtomicBoolean running = new AtomicBoolean(false);
    private int order = 0;
    private AtomicInteger port = new AtomicInteger(0);
    private ApplicationContext context;
    private PantheonServiceRegistry serviceRegistry;
    private PantheonRegistration registration;

    public PantheonAutoServiceRegistration(ApplicationContext context, PantheonServiceRegistry serviceRegistry, PantheonRegistration registration) {
        this.context = context;
        this.serviceRegistry = serviceRegistry;
        this.registration = registration;
    }

    public void start() {
        if (this.port.get() != 0) {
            if (this.registration.getNonSecurePort() == 0) {
                this.registration.setNonSecurePort(this.port.get());
            }
        }

        if (!this.running.get() && this.registration.getNonSecurePort() > 0) {
            this.serviceRegistry.register(this.registration);
            this.context.publishEvent(new InstanceRegisteredEvent(this, this.registration.getInstanceConfig()));
            this.running.set(true);
        }

    }

    public void stop() {
        this.serviceRegistry.deregister(this.registration);
        this.running.set(false);
    }

    public boolean isRunning() {
        return this.running.get();
    }

    public int getPhase() {
        return 0;
    }

    public boolean isAutoStartup() {
        return true;
    }

    public void stop(Runnable callback) {
        this.stop();
        callback.run();
    }

    public int getOrder() {
        return this.order;
    }

    @EventListener({EmbeddedServletContainerInitializedEvent.class})
    public void onApplicationEvent(EmbeddedServletContainerInitializedEvent event) {
        int localPort = event.getEmbeddedServletContainer().getPort();
        if (this.port.get() == 0) {
            log.info("Updating port to " + localPort);
            this.port.compareAndSet(0, localPort);
            this.start();
        }

    }

    @EventListener({ContextClosedEvent.class})
    public void onApplicationEvent(ContextClosedEvent event) {
        if (event.getApplicationContext() == this.context) {
            this.stop();
        }

    }
}
