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
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    private final Lock lockHeartbeat = new ReentrantLock();
    private final String clientId;


    public InstanceNode(NettyClientConfig nettyClientConfig, DefaultInstanceConfig instanceConfig, String clientId) {
        this.nettyClientConfig = nettyClientConfig;
        this.instanceConfig = instanceConfig;
        this.clientId = clientId;
        clientAPI = new ClientAPIImpl(nettyClientConfig, instanceConfig, new ClientRemotingProcessor(), null);
    }


    public boolean start() {
        clientAPI.start();

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

            this.startScheduledTask();
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
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                sendHeartBeatToServer(server);
            }
        }, 1000, instanceConfig.getLeaseRenewalIntervalInSeconds() * 1000, TimeUnit.MILLISECONDS);
        //heartbeat
    }

    private void sendHeartBeatToServer(Server server) {
        if (this.lockHeartbeat.tryLock()) {
            try {
                boolean successResult = this.clientAPI.sendHeartBeatToServer(server, clientId,3000L);
                if (successResult){
                    logger.info("heartbeat success!!!");
                }
            } catch (final Exception e) {
                logger.error("sendHeartBeatToServer exception", e);
            } finally {
                this.lockHeartbeat.unlock();
            }
        } else {
            logger.warn("lock heartBeat, but failed. [{}]", instanceConfig.getServiceName());
        }
    }

    public void shutdown() {
        this.clientAPI.shutdown();
    }

    public void sendRegister() {
        try {
            boolean registryResult = this.clientAPI.serviceRegistry(server.getRemoteSocketAddress(), 1000);
            if (registryResult) {
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
