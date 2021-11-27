package com.pantheon.client;

import com.alibaba.fastjson.JSON;
import com.pantheon.client.config.DefaultInstanceConfig;
import com.pantheon.client.config.PantheonInstanceConfig;
import com.pantheon.common.protocol.RequestCode;
import com.pantheon.common.protocol.ResponseCode;
import com.pantheon.common.protocol.header.*;
import com.pantheon.remoting.CommandCustomHeader;
import com.pantheon.remoting.InvokeCallback;
import com.pantheon.remoting.RPCHook;
import com.pantheon.remoting.RemotingClient;
import com.pantheon.remoting.annotation.CFNullable;
import com.pantheon.remoting.exception.*;
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

/**
 * @author Anthony
 * @create 2021/11/27
 * @desc wrapper class for netty request and response
 */
public class ClientAPIImpl {
    private static final Logger logger = LoggerFactory.getLogger(ClientAPIImpl.class);

    private final ClientRemotingProcessor clientRemotingProcessor;
    private final RemotingClient remotingClient;
    private String serverAddress = null;
    private NettyClientConfig nettyClientConfig;
    private PantheonInstanceConfig pantheonInstanceConfig;
    private Map<String, List<String>> slotsAllocation;
    private String excludedRemoteAddress;
    private Server server;
    private static final Integer SLOT_COUNT = 16384;
    private DefaultInstanceConfig instanceConfig;

    public ClientAPIImpl(final NettyClientConfig nettyClientConfig, DefaultInstanceConfig instanceConfig, ClientRemotingProcessor clientRemotingProcessor, RPCHook rpcHook) {
        this.clientRemotingProcessor = clientRemotingProcessor;
        this.nettyClientConfig = nettyClientConfig;
        this.instanceConfig = instanceConfig;
        this.remotingClient = new NettyRemotingClient(nettyClientConfig, null);
        this.pantheonInstanceConfig = DefaultInstanceConfig.getInstance();
        this.remotingClient.registerRPCHook(rpcHook);
        this.remotingClient.registerProcessor(RequestCode.GET_CONSUMER_RUNNING_INFO, this.clientRemotingProcessor, null);

    }

    public void start() {
        this.remotingClient.start();

    }


    public void shutdown() {
        this.remotingClient.shutdown();
    }

    /**
     * fetch server node id
     *
     * @param controllerCandidate server address
     * @return
     */
    public Integer fetchServerNodeId(final String controllerCandidate, final long timoutMills) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException {
        GetServerNodeIdRequestHeader requestHeader = new GetServerNodeIdRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SERVER_NODE_ID, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(controllerCandidate, request, timoutMills);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetServerNodeIdResponseHeader responseHeader =
                        (GetServerNodeIdResponseHeader) response.decodeCommandCustomHeader(GetServerNodeIdResponseHeader.class);
                return responseHeader.getServerNodeId();
            }
            default:
                break;
        }
        return null;
    }

    /**
     * fetch slots allocation
     *
     * @param controllerCandidate
     * @return
     */
    public Map<String, List<String>> fetchSlotsAllocation(final String controllerCandidate, final long timoutMills) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException {
        GetSlotsRequestHeader requestHeader = new GetSlotsRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SLOTS_ALLOCATION, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(controllerCandidate, request, timoutMills);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetSlotsResponseHeader responseHeader =
                        (GetSlotsResponseHeader) response.decodeCommandCustomHeader(GetSlotsResponseHeader.class);
                slotsAllocation = (Map<String, List<String>>) JSON.parse(responseHeader.getSlotsAllocation());
                return slotsAllocation;
            }
            default:
                break;
        }
        return null;
    }


    public boolean serviceRegistry(final String serverAddress, final long timoutMills) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        ServiceRegistryRequestHeader requestHeader =new ServiceRegistryRequestHeader();
        requestHeader.setServiceName(instanceConfig.getServiceName());
        requestHeader.setServiceInstancePort(instanceConfig.getInstancePort());
        requestHeader.setServiceInstanceIp(instanceConfig.getInstanceIpAddress());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SERVICE_REGISTRY, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(serverAddress, request, timoutMills);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return true;
            }
            default:
                break;
        }
        return false;
    }

    /**
     * server地址列表
     */
    private Map<String/*server node id*/, Server> servers = new HashMap<String, Server>();

    /**
     * fetch server addresses to local map
     *
     * @param controllerCandidate
     */
    public Map<String, Server> fetchServerAddresses(String controllerCandidate, final long timoutMills) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException {
        GetServerAddressRequestHeader requestHeader = new GetServerAddressRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SERVER_ADDRESSES, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(controllerCandidate, request, timoutMills);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetServerAddressResponseHeader responseHeader =
                        (GetServerAddressResponseHeader) response.decodeCommandCustomHeader(GetServerAddressResponseHeader.class);
                List<String> serverAddresses = (List<String>) JSON.parse(responseHeader.getServerAddresses());
                for (String serverAddress : serverAddresses) {
                    String[] serverAddressSplited = serverAddress.split(":");

                    String id = serverAddressSplited[0];

                    String ip = serverAddressSplited[1];
                    Integer port = Integer.valueOf(serverAddressSplited[2]);
                    Server server = new Server(id, ip, port);

                    servers.put(id, server);
                    return servers;
                }
            }
            default:
                break;
        }
        return null;
    }

    public Server routeServer(String serviceName) {
        Integer slot = routeSlot(serviceName);
        String serverId = locateServerBySlot(slot);
        Server server = servers.get(serverId);
        logger.info(serviceName + " route to serverId: {}", serverId);
        logger.info(serviceName + " route to slot: {} to server: {}", slot, server);
        return server;
    }

    /**
     * route server to a specific slot
     *
     * @return
     */
    private Integer routeSlot(String serviceName) {
        int hashCode = serviceName.hashCode() & Integer.MAX_VALUE;
        Integer slot = hashCode % SLOT_COUNT;

        if (slot == 0) {
            slot = slot + 1;
        }

        return slot;
    }


    public void invokeSync() throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        logger.info("sync message to " + instanceConfig.getServerList().get(0));
        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setCount(1);
        requestHeader.setMessageTitle("Welcome");
        RemotingCommand request = RemotingCommand.createRequestCommand(0, requestHeader);
        RemotingCommand response = remotingClient.invokeSync(instanceConfig.getServerList().get(0), request, 1000 * 3);
        System.out.println(response);
    }


    public void invokeOneway() throws InterruptedException, RemotingConnectException,
            RemotingTimeoutException, RemotingTooMuchRequestException, RemotingSendRequestException {

        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        request.setRemark("messi");
        remotingClient.invokeOneway(instanceConfig.getServerList().get(0), request, 1000 * 3);
    }


    public void invokeAsync() throws InterruptedException, RemotingConnectException,
            RemotingTimeoutException, RemotingTooMuchRequestException, RemotingSendRequestException {

        final CountDownLatch latch = new CountDownLatch(1);
        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        request.setRemark("messi");
        remotingClient.invokeAsync(instanceConfig.getServerList().get(0), request, 1000 * 3, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {
                latch.countDown();
                System.out.println(responseFuture.getResponseCommand());
            }
        });
        latch.await();
    }


    /**
     * locate a server node id by slot
     *
     * @param slot
     * @return
     */
    private String locateServerBySlot(Integer slot) {
        for (String serverNodeId : slotsAllocation.keySet()) {
            List<String> slotsList = slotsAllocation.get(serverNodeId);

            for (String slots : slotsList) {
                String[] slotsSpited = slots.split(",");
                Integer startSlot = Integer.valueOf(slotsSpited[0]);
                Integer endSlot = Integer.valueOf(slotsSpited[1]);

                if (slot >= startSlot && slot <= endSlot) {
                    return serverNodeId;
                }
            }
        }
        return null;
    }


    public String chooseControllerCandidate() {
        List<String> serverList = instanceConfig.getServerList();
        Random random = new Random();
        boolean chosen = false;
        while (!chosen) {
            int index = random.nextInt(serverList.size());
            String serverAddress = serverList.get(index);

            if (excludedRemoteAddress == null) {
                return serverAddress;
            } else {
                if (serverAddress.equals(excludedRemoteAddress)) {
                    continue;
                } else {
                    return serverAddress;
                }
            }
        }

        return null;
    }


    static class RequestHeader implements CommandCustomHeader {
        @CFNullable
        private Integer count;

        @CFNullable
        private String messageTitle;

        @Override
        public void checkFields() throws RemotingCommandException {
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        public String getMessageTitle() {
            return messageTitle;
        }

        public void setMessageTitle(String messageTitle) {
            this.messageTitle = messageTitle;
        }
    }

}
