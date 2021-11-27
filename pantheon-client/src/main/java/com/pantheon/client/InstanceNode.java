package com.pantheon.client;

import com.alibaba.fastjson.JSON;
import com.pantheon.client.config.DefaultInstanceConfig;
import com.pantheon.common.protocol.RequestCode;
import com.pantheon.common.ThreadFactoryImpl;
import com.pantheon.common.protocol.ResponseCode;
import com.pantheon.common.protocol.header.GetServerAddressRequestHeader;
import com.pantheon.common.protocol.header.GetServerAddressResponseHeader;
import com.pantheon.common.protocol.header.GetServerNodeIdRequestHeader;
import com.pantheon.common.protocol.header.GetServerNodeIdResponseHeader;
import com.pantheon.common.protocol.header.GetSlotsRequestHeader;
import com.pantheon.common.protocol.header.GetSlotsResponseHeader;
import com.pantheon.remoting.CommandCustomHeader;
import com.pantheon.remoting.InvokeCallback;
import com.pantheon.remoting.annotation.CFNullable;
import com.pantheon.remoting.exception.RemotingCommandException;
import com.pantheon.remoting.exception.RemotingConnectException;
import com.pantheon.remoting.exception.RemotingSendRequestException;
import com.pantheon.remoting.exception.RemotingTimeoutException;
import com.pantheon.remoting.exception.RemotingTooMuchRequestException;
import com.pantheon.remoting.netty.NettyClientConfig;
import com.pantheon.remoting.netty.NettyRemotingClient;
import com.pantheon.remoting.netty.ResponseFuture;
import com.pantheon.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.ServerBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
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



    public InstanceNode(NettyClientConfig nettyClientConfig, DefaultInstanceConfig instanceConfig) {
        this.nettyClientConfig = nettyClientConfig;
        this.instanceConfig = instanceConfig;
        clientAPI = new ClientAPIImpl(nettyClientConfig,instanceConfig, new ClientRemotingProcessor(), null);
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
            this.clientAPI.routeServer(serviceName);
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

}
