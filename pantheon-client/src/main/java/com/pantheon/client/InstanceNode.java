package com.pantheon.client;

import com.pantheon.client.config.DefaultInstanceConfig;
import com.pantheon.common.ThreadFactoryImpl;
import com.pantheon.remoting.exception.RemotingCommandException;
import com.pantheon.remoting.exception.RemotingConnectException;
import com.pantheon.remoting.exception.RemotingSendRequestException;
import com.pantheon.remoting.exception.RemotingTimeoutException;
import com.pantheon.remoting.netty.NettyClientConfig;
import io.netty.bootstrap.ServerBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Anthony
 * @create 2021/11/19
 * @desc
 **/
public class InstanceNode {
    private NettyClientConfig nettyClientConfig;
    private DefaultInstanceConfig instanceConfig;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "InstanceControllerScheduledThread"));
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);
    private ClientAPIImpl clientAPI;
    private Server server;

    public InstanceNode(NettyClientConfig nettyClientConfig, DefaultInstanceConfig instanceConfig) {
        this.nettyClientConfig = nettyClientConfig;
        this.instanceConfig = instanceConfig;
        clientAPI = new ClientAPIImpl(nettyClientConfig, instanceConfig, new ClientRemotingProcessor(), null);
    }


    public boolean start() {
        clientAPI.start();
        this.startScheduledTask();
        //choose a controller candidate from local config
        String controllerCandidate = this.clientAPI.chooseControllerCandidate();
        Integer nodeId = null;
        try {
            nodeId = this.clientAPI.fetchServerNodeId(controllerCandidate, 10000);
            logger.info("fetchServerNodeId successful load nodeId: " + nodeId);
            Map<String, List<String>> integerListMap = this.clientAPI.fetchSlotsAllocation(controllerCandidate, 10000);
            logger.info("fetchSlotsAllocation successful load map: " + integerListMap);

            Map<String, Server> serverMap = this.clientAPI.fetchServerAddresses(controllerCandidate, 1000);
            logger.info("fetchServerAddresses successful load map: " + serverMap);

            String serviceName = instanceConfig.getServiceName();
            server = this.clientAPI.routeServer(serviceName);
        } catch (RemotingConnectException e) {
            e.printStackTrace();
        } catch (RemotingSendRequestException e) {
            e.printStackTrace();
        } catch (RemotingTimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (RemotingCommandException e) {
            e.printStackTrace();
        }
        return true;
    }

    private void startScheduledTask() {
        //heartbeat
    }

    public void shutdown() {
        this.clientAPI.shutdown();
    }

    public void sendRegister() {
        try {
            boolean registryResult = this.clientAPI.serviceRegistry(server.getRemoteSocketAddress(), 1000);
            if(registryResult){
                logger.info("service registry success!!!");
            }
        } catch (RemotingConnectException e) {
            e.printStackTrace();
        } catch (RemotingSendRequestException e) {
            e.printStackTrace();
        } catch (RemotingTimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
