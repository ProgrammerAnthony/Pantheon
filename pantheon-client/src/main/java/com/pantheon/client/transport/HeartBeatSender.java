package com.pantheon.client.transport;

/**
 * @author Anthony
 * @create 2022/1/5
 * @desc The heartbeat sender which is responsible for sending heartbeat to remote server periodically per interval.
 */
public interface HeartBeatSender {

    boolean sendHeartbeat();

    long intervalMs();
}
