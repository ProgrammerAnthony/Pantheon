package com.pantheon.client;

/**
 * Listener for receiving {@link ClientNode} events such as {@link StatusChangeEvent}.  Register
 * a listener by calling {@link ClientNode#registerEventListener(PantheonEventListener)}
 */
public interface PantheonEventListener {
    /**
     * Notification of an event within the {@link ClientNode}.
     * 
     * {@link PantheonEventListener#onEvent} is called from the context of an internal eureka thread
     * and must therefore return as quickly as possible without blocking.
     * 
     * @param event
     */
    public void onEvent(PantheonEvent event);
}
