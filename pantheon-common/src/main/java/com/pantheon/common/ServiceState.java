package com.pantheon.common;

public enum ServiceState {
    /**
     * Service just created,not start
     */
    CREATE_JUST,
    /**
     * Service Running
     */
    RUNNING,
    /**
     * Service shutdown
     */
    SHUTDOWN_ALREADY,
    /**
     * Service Start failure
     */
    START_FAILED,

    /**
     * Service running failure
     */
    SERVICE_FAILED;
}
