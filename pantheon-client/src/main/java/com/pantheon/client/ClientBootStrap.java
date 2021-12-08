package com.pantheon.client;

import com.pantheon.client.config.DefaultInstanceConfig;
import com.pantheon.common.ShutdownHookThread;
import com.pantheon.common.lifecycle.Lifecycle;
import com.pantheon.remoting.RemotingClient;
import com.pantheon.remoting.netty.NettyClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * @author Anthony
 * @create 2021/11/17
 * @desc
 **/
public class ClientBootStrap {
    private static final Logger logger = LoggerFactory.getLogger(ClientBootStrap.class);

    public static void main(String[] args) {
        logger.info("InstanceBootStrap initializing......");
        ClientNode clientNode = ClientManager.getInstance().getOrCreateClientNode(DefaultInstanceConfig.getInstance());

       clientNode.start();
        if (!clientNode.lifecycleState().equals(Lifecycle.State.STARTED)) {
            clientNode.shutdown();
            System.exit(-3);
        }
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(logger, (Callable<Void>) () -> {
            clientNode.shutdown();
            return null;
        }));
    }

    public static void shutdown(final ClientNode clientNode) {
        clientNode.stop();
    }

}
