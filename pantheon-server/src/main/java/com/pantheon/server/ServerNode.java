package com.pantheon.server;

import com.pantheon.common.ServerNodeRole;
import com.pantheon.common.ThreadFactoryImpl;
import com.pantheon.common.lifecycle.AbstractLifecycleComponent;
import com.pantheon.common.protocol.RequestCode;
import com.pantheon.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import com.pantheon.remoting.RemotingServer;
import com.pantheon.remoting.exception.RemotingCommandException;
import com.pantheon.remoting.netty.NettyRemotingServer;
import com.pantheon.remoting.netty.NettyServerConfig;
import com.pantheon.remoting.protocol.RemotingCommand;
import com.pantheon.server.client.ClientHousekeepingService;
import com.pantheon.server.client.ConsumerInfoManager;
import com.pantheon.server.client.ServerToClient;
import com.pantheon.server.config.CachedPantheonServerConfig;
import com.pantheon.server.config.PantheonServerConfig;
import com.pantheon.server.network.ServerMessageReceiver;
import com.pantheon.server.network.ServerNetworkManager;
import com.pantheon.server.node.*;
import com.pantheon.server.processor.ClientManageProcessor;
import com.pantheon.server.processor.ServerNodeProcessor;
import com.pantheon.server.slot.SlotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * @author Anthony
 * @create 2021/11/18
 * @desc 1 todo design slots mechanism and treat it as something like topic in RocketMq
 **/
public class ServerNode extends AbstractLifecycleComponent {
    private final ThreadPoolExecutor heartbeatExecutor;
    private final ThreadPoolExecutor serviceManageExecutor;
    private final ServerToClient serverToClient;
    private final ConsumerInfoManager consumerInfoManager;
    private final ClientHousekeepingService clientHousekeepingService;
    private NettyServerConfig nettyServerConfig;
    private PantheonServerConfig serverConfig;
    private RemotingServer remotingServer;
    private ExecutorService remotingExecutor;
    private ClientManageProcessor clientManageProcessor;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "ServerControllerScheduledThread"));
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);

    private volatile static ServerNodeRole serverNodeRole = ServerNodeRole.COMMON_NODE;

    private ServerNode() {
        this.nettyServerConfig = new NettyServerConfig();
        this.serverConfig = CachedPantheonServerConfig.getInstance();
        this.serverToClient = new ServerToClient(this);
        this.consumerInfoManager = new ConsumerInfoManager(this);
        this.clientHousekeepingService = new ClientHousekeepingService(this);
        this.heartbeatExecutor = new ThreadPoolExecutor(
                1,
                1,
                1000 * 60,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(5000),
                new ThreadFactoryImpl("InstanceHeartbeatThread_", true));

        this.serviceManageExecutor = new ThreadPoolExecutor(
                1,
                1,
                1000 * 60,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(5000),
                new ThreadFactoryImpl("ServiceManageThread_", true));
    }

    private static class Singleton {
        static ServerNode instance = new ServerNode();
    }

    public static ServerNode getInstance() {
        return ServerNode.Singleton.instance;
    }

    private void startNettyClientServer() {
        nettyServerConfig.setListenPort(serverConfig.getNodeClientTcpPort());
        remotingServer = new NettyRemotingServer(nettyServerConfig, this.clientHousekeepingService);
        this.remotingExecutor =
                Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));
        ServerNodeProcessor defaultProcessor = new ServerNodeProcessor(this);
        remotingServer.registerDefaultProcessor(defaultProcessor, remotingExecutor);
        //use threadpool to process management event
        clientManageProcessor = new ClientManageProcessor(this);
        remotingServer.registerProcessor(RequestCode.SERVICE_HEART_BEAT, clientManageProcessor, heartbeatExecutor);
        remotingServer.registerProcessor(RequestCode.GET_ALL_APP, clientManageProcessor, heartbeatExecutor);
        remotingServer.registerProcessor(RequestCode.GET_DELTA_APP, clientManageProcessor, heartbeatExecutor);
        remotingServer.registerProcessor(RequestCode.SERVICE_UNREGISTER, clientManageProcessor, heartbeatExecutor);
        remotingServer.registerProcessor(RequestCode.SERVICE_REGISTRY, clientManageProcessor, heartbeatExecutor);

        remotingServer.start();
        logger.info("server with id :{}, listen to client connection on tcp port :{}", serverConfig.getNodeId(), serverConfig.getNodeClientTcpPort());
    }

    private boolean manageServerNetwork(Boolean isControllerCandidate) {
        //listener for other servers' connection
        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        serverNetworkManager.startServerConnectionListener();

        // controller candidate
        if (isControllerCandidate) {
            // connect before candidates (not all) avoid of nodes' duplicate connection
            if (!serverNetworkManager.connectBeforeControllerCandidateServers()) {
                ServerNode.this.stop();
                return false;
            }
            //wait for other nodes' connection
            serverNetworkManager.waitAllControllerCandidatesConnected();
            serverNetworkManager.waitAllServerNodeConnected();
        }
        // common node will connect to all controller node
        else {
            serverNetworkManager.connectAllControllerCandidates();
        }
        // message receiver to receive messages from servers
        ServerMessageReceiver serverMessageReceiver = ServerMessageReceiver.getInstance();
        serverMessageReceiver.start();
        return true;
    }

    private boolean controllerCandidateInit(ServerNodeRole serverNodeRole) {
        // controller candidate elect controller with votes
        ControllerCandidate controllerCandidate = ControllerCandidate.getInstance();
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        //controller exists
        if (remoteServerNodeManager.hasController()) {
            RemoteServerNode controller = remoteServerNodeManager.getController();
            // ask controller to sync slots data
            controllerCandidate.requestSlotsData(controller.getNodeId());
            controllerCandidate.waitForSlotsAllocation();
            controllerCandidate.waitForSlotsReplicaAllocation();
            controllerCandidate.waitReplicaNodeIds();
        } else {
            serverNodeRole = controllerCandidate.electController();
            logger.info("after election, the role of this server node：" + (serverNodeRole == ServerNodeRole.CONTROLLER_NODE ? "Controller" : "Controller candidate"));

            if (serverNodeRole == ServerNodeRole.CONTROLLER_NODE) {
                //controller determine slots allocation
                Controller controller = Controller.getInstance();
                controller.allocateSlots();
                controller.initControllerNode();
                controller.broadcastControllerId();
            } else if (serverNodeRole == ServerNodeRole.CONTROLLER_CANDIDATE_NODE) {
                //controller candidate wait for slots allocation
                controllerCandidate.waitForSlotsAllocation();
                controllerCandidate.waitForSlotsReplicaAllocation();
                controllerCandidate.waitReplicaNodeIds();
            }
        }
        setServerRole(serverNodeRole);
        return true;
    }

    private void startScheduledTask() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    ServerNode.this.getConsumerRunningInfo();
                } catch (Exception e) {
                    logger.error("ScheduledTask getConsumerRunningInfo exception", e);
                }
            }
        }, 5000, serverConfig.getHeartBeatCheckInterval(), TimeUnit.MILLISECONDS);
        //heartbeat check with connected instances，remove expired ones
//        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
//
//            @Override
//            public void run() {
//                try {
//                    ServerNode.this.heartBeatCheck();
//                } catch (Exception e) {
//                    logger.error("ScheduledTask heartBeatCheck exception", e);
//                }
//            }
//        }, 5000, serverConfig.getHeartBeatCheckInterval(), TimeUnit.MILLISECONDS);

    }

    private void getConsumerRunningInfo() throws RemotingCommandException {
        if (clientManageProcessor != null) {
            final GetConsumerRunningInfoRequestHeader requestHeader = new GetConsumerRunningInfoRequestHeader();
            RemotingCommand request =
                    RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO,
                            requestHeader);
            clientManageProcessor.callConsumer(RequestCode.GET_CONSUMER_RUNNING_INFO, request);
        }

    }


    //todo heartbeat check and cache evict when timeout
    private void heartBeatCheck() {

    }


    public PantheonServerConfig getServerConfig() {
        return serverConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }


//    public static boolean isRunning() {
//        return getServiceState() == ServiceState.RUNNING;
//    }

    public static boolean isController() {
        return getServerNodeRole() == ServerNodeRole.CONTROLLER_NODE;
    }

    public static ServerNodeRole setServerRole(ServerNodeRole serverNodeRole) {
        ServerNode.serverNodeRole = serverNodeRole;
        return ServerNode.serverNodeRole;
    }

    public static ServerNodeRole getServerNodeRole() {
        return serverNodeRole;
    }

    @Override
    protected void doStart() {
        ServerNodeRole serverNodeRole = ServerNodeRole.COMMON_NODE;

        this.startScheduledTask();

        Boolean isControllerCandidate = serverConfig.isControllerCandidate();

        manageServerNetwork(isControllerCandidate);
        if (isControllerCandidate) {
            controllerCandidateInit(serverNodeRole);
        }

        //master node (not controller) wait for slots data
        if (!isController()) {
            SlotManager slotManager = SlotManager.getInstance();
            slotManager.initSlots(null);
            slotManager.initSlotsReplicas(null, false);
            slotManager.initReplicaNodeId(null);
            ControllerNode.setNodeId(ServerMessageReceiver.getInstance().takeControllerNodeId());
        }

        startNettyClientServer();

        //save connection between client and server
        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.start();
        }
    }

    @Override
    protected void doStop() {
        if (remotingServer != null) {
            this.remotingServer.shutdown();
        }
        if (remotingExecutor != null) {
            this.remotingExecutor.shutdown();
        }
        if (clientHousekeepingService != null) {
            this.clientHousekeepingService.shutdown();
        }
        logger.info("the server controller shutdown OK");
    }

    @Override
    protected void doClose() throws IOException {

    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public ServerToClient getServerToClient() {
        return serverToClient;
    }

    public ConsumerInfoManager getConsumerInfoManager() {
        return consumerInfoManager;
    }
}
