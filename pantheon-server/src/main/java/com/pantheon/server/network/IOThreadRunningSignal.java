package com.pantheon.server.network;

/**
 * io线程运行信号量
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
