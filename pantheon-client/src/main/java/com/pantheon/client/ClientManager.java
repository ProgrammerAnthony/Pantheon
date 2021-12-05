package com.pantheon.client;

import com.pantheon.client.config.DefaultInstanceConfig;
import com.pantheon.remoting.netty.NettyClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Anthony
 * @create 2021/12/5
 * @desc singleton to manage all clients
 */
public class ClientManager {
    private static final Logger logger = LoggerFactory.getLogger(ClientManager.class);

    private ConcurrentMap<String/* clientId */, ClientNode> factoryTable =
            new ConcurrentHashMap<>();
    private NettyClientConfig nettyClientConfig;

    private static ClientManager instance = new ClientManager();
    private DefaultInstanceConfig instanceConfig;

    private ClientManager() {

    }

    public static ClientManager getInstance() {
        return instance;
    }

    public ClientNode getOrCreateClientNode(DefaultInstanceConfig instanceConfig) {
        this.instanceConfig=instanceConfig;
        nettyClientConfig = new NettyClientConfig();

        String clientId = getInstance().buildClientId();
        ClientNode instance = this.factoryTable.get(clientId);
        if (null == instance) {
            instance = new ClientNode(nettyClientConfig, instanceConfig, clientId);
            ClientNode prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {
                instance = prev;
                logger.warn("Returned Previous ClientNode for clientId:[{}]", clientId);
            } else {
                logger.info("Created new ClientNode for clientId:[{}]", clientId);
            }
        }

        return instance;

    }

    public String buildClientId() {
        StringBuilder sb = new StringBuilder();
        sb.append(instanceConfig.getInstanceIpAddress());

        sb.append(":");
        sb.append(instanceConfig.getServiceName());
        sb.append(":");
        sb.append(instanceConfig.getInstancePort());

        return sb.toString();
    }
}