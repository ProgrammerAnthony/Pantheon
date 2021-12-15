package com.pantheon.client;

/**
 * Listener for receiving {@link DiscoveryClientNode} events such as {@link StatusChangeEvent}.  Register
 * a listener by calling {@link DiscoveryClientNode#registerEventListener(PantheonEventListener)}
 */
public interface PantheonEventListener {
    /**
     * Notification of an event within the {@link DiscoveryClientNode}.
     * 
     * {@link PantheonEventListener#onEvent} is called from the context of an internal eureka thread
     * and must therefore return as quickly as possible without blocking.
     * 
     * @param event
     */
    public void onEvent(PantheonEvent event);
}
