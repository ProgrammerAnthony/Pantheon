package com.pantheon.server.network;

/**
 * io thread running signal
 */
public class IOThreadRunningSignal {

    private volatile Boolean isRunning;

    public IOThreadRunningSignal(Boolean isRunning) {
        this.isRunning = isRunning;
    }

    public Boolean isRunning() {
        return isRunning;
    }

    public void setIsRunning(Boolean isRunning) {
        this.isRunning = isRunning;
    }

}
