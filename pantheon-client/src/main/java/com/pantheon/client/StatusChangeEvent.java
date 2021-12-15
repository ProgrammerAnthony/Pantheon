package com.pantheon.client;

import com.pantheon.client.appinfo.InstanceInfo;

/**
 * Event containing the latest instance status information.  This event
 * is sent to the {@link com.netflix.eventbus.spi.EventBus} by {@link DiscoveryClientNode ) whenever
 * a status change is identified from the remote Eureka server response.
 */
public class StatusChangeEvent {
    // System time when the event happened
    private final long timestamp;
    private final InstanceInfo.InstanceStatus current;
    private final InstanceInfo.InstanceStatus previous;

    public StatusChangeEvent(InstanceInfo.InstanceStatus previous, InstanceInfo.InstanceStatus current) {
        this.timestamp = System.currentTimeMillis();
        this.current = current;
        this.previous = previous;
    }


    /**
     * @return Return the system time in milliseconds when the event happened.
     */
    public final long getTimestamp() {
        return this.timestamp;
    }

    /**
     * Return the up current when the event was generated.
     * @return true if current is up or false for ALL other current values
     */
    public boolean isUp() {
        return this.current.equals(InstanceInfo.InstanceStatus.UP);
    }

    /**
     * @return The current at the time the event is generated.
     */
    public InstanceInfo.InstanceStatus getStatus() {
        return current;
    }

    /**
     * @return Return the client status immediately before the change
     */
    public InstanceInfo.InstanceStatus getPreviousStatus() {
        return previous;
    }

    @Override
    public String toString() {
        return "StatusChangeEvent [timestamp=" + getTimestamp() + ", current=" + current + ", previous="
                + previous + "]";
    }

}
