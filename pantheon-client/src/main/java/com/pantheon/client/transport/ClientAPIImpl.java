package com.pantheon.client.transport;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pantheon.client.appinfo.Applications;
import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.client.config.DefaultInstanceConfig;
import com.pantheon.client.config.PantheonInstanceConfig;
import com.pantheon.common.protocol.RequestCode;
import com.pantheon.common.protocol.ResponseCode;
import com.pantheon.common.protocol.header.*;
import com.pantheon.common.protocol.heartBeat.HeartBeat;
import com.pantheon.remoting.RPCHook;
import com.pantheon.remoting.RemotingClient;
import com.pantheon.remoting.exception.*;
import com.pantheon.remoting.netty.NettyClientConfig;
import com.pantheon.remoting.netty.NettyRemotingClient;
import com.pantheon.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.zip.GZIPInputStream;

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
    private Map<String/*serverNodeId*/, List<String>> slotsAllocation;
    private static final Integer SLOT_COUNT = 16384;
    private PantheonInstanceConfig instanceConfig;
    /**
     * server addresses
     */
    private Map<String/*server node id*/, Server> servers = new HashMap<String, Server>();

    public ClientAPIImpl(final NettyClientConfig nettyClientConfig, PantheonInstanceConfig instanceConfig, ClientRemotingProcessor clientRemotingProcessor, RPCHook rpcHook) {
        this.clientRemotingProcessor = clientRemotingProcessor;
        this.nettyClientConfig = nettyClientConfig;
        this.instanceConfig = instanceConfig;
        this.remotingClient = new NettyRemotingClient(nettyClientConfig, null);
        this.pantheonInstanceConfig = DefaultInstanceConfig.getInstance();
        this.remotingClient.registerRPCHook(rpcHook);
        this.remotingClient.registerProcessor(RequestCode.GET_CONSUMER_RUNNING_INFO, this.clientRemotingProcessor, null);
    }

    public void start() {
        if (this.remotingClient != null) {
            this.remotingClient.start();
            return;
        }
        throw new RuntimeException("remotingClient cannot be null");

    }


    public void shutdown() {
        if (remotingClient != null) {
            this.remotingClient.shutdown();
        }
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
                }
                return servers;
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
        server.setSlotNum(slot);
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
            return serverAddress;
        }

        return null;
    }

    public boolean sendHeartBeatToServer(final Server server, String appName, String id, final Long timoutMills) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        HeartBeat heartBeat = new HeartBeat();
        heartBeat.setServiceName(appName);
        heartBeat.setInstanceId(id);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SERVICE_HEART_BEAT, null);
        request.setBody(heartBeat.encode());
        RemotingCommand response = this.remotingClient.invokeSync(server.getRemoteSocketAddress(), request, timoutMills);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                logger.info("heartbeat success!!!");
                return true;
            }
            case ResponseCode.SYSTEM_ERROR: {
                logger.info("heartbeat failed!!! {}", response.getRemark());
            }
            default:
                break;
        }
        return false;
    }


    public Applications getApplications(Server server, long timeoutMills) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, IOException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_APP, null);
        RemotingCommand response = this.remotingClient.invokeSync(server.getRemoteSocketAddress(), request, timeoutMills);

        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                String string = unzipBytesToStr(response.getBody());
                logger.info("receive all apps info :{} ", string);
                return JSONObject.parseObject(string, Applications.class);
            }
            default:
                break;
        }
        return null;
    }


    public String unzipBytesToStr(byte[] inputBuf) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(inputBuf);
        // Open the compressed stream
        GZIPInputStream gin = new GZIPInputStream(bis);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        // Transfer bytes from the compressed stream to the output stream
        byte[] buf = new byte[inputBuf.length];
        int len;
        while ((len = gin.read(buf)) > 0) {
            out.write(buf, 0, len);
        }

        // Close the file and stream
        gin.close();
        out.close();

        byte[] bytes = out.toByteArray();
        String string = new String(bytes);
        return string;
    }

    public Applications getDelta(Server server, long timeoutMills) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, IOException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_DELTA_APP, null);
        RemotingCommand response = this.remotingClient.invokeSync(server.getRemoteSocketAddress(), request, timeoutMills);

        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                String string = unzipBytesToStr(response.getBody());
                logger.info("receive delta apps info :{} ", string);

                return JSONObject.parseObject(string, Applications.class);
            }
            default:
                break;
        }
        return null;
    }

    public boolean register(Server server, InstanceInfo instanceInfo, long timoutMills) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SERVICE_REGISTRY, null);
        request.setBody(instanceInfo.encode());
        RemotingCommand response = this.remotingClient.invokeSync(server.getRemoteSocketAddress(), request, timoutMills);
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

    public boolean unRegister(Server server, String appName, String instanceId, long timoutMills) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        HeartBeat heartBeat = new HeartBeat();
        heartBeat.setInstanceId(instanceId);
        heartBeat.setServiceName(appName);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SERVICE_UNREGISTER, null);
        request.setBody(heartBeat.encode());
        RemotingCommand response = this.remotingClient.invokeSync(server.getRemoteSocketAddress(), request, timoutMills);
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
}
