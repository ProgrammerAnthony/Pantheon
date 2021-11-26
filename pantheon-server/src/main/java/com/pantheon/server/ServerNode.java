package com.pantheon.server;

import com.pantheon.common.ServerNodeRole;
import com.pantheon.common.ThreadFactoryImpl;
import com.pantheon.common.lifecycle.AbstractLifecycleComponent;
import com.pantheon.remoting.RemotingServer;
import com.pantheon.remoting.netty.NettyRemotingServer;
import com.pantheon.remoting.netty.NettyServerConfig;
import com.pantheon.server.config.CachedPantheonServerConfig;
import com.pantheon.server.config.PantheonServerConfig;
import com.pantheon.server.network.ServerMessageReceiver;
import com.pantheon.server.network.ServerNetworkManager;
import com.pantheon.server.node.*;
import com.pantheon.server.processor.ServerNodeProcessor;
import com.pantheon.server.slot.SlotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Anthony
 * @create 2021/11/18
 * @desc 2 todo rethink and build the mechanism of client and server RocketMq
 * 3 todo build heartbeat mechanism of client and server
 * 4 todo design the message protocol
 * 5 todo design slots mechanism and treat it as topic in RocketMq
 **/
public class ServerNode extends AbstractLifecycleComponent {
    private NettyServerConfig nettyServerConfig;
    private PantheonServerConfig serverConfig;
    private RemotingServer remotingServer;
    private ExecutorService remotingExecutor;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "ServerControllerScheduledThread"));
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);

    private volatile static ServerNodeRole serverNodeRole = ServerNodeRole.COMMON_NODE;

    private ServerNode() {
        this.nettyServerConfig = new NettyServerConfig();
        this.serverConfig = CachedPantheonServerConfig.getInstance();
    }

    private static class Singleton {
        static ServerNode instance = new ServerNode();
    }

    public static ServerNode getInstance() {
        return ServerNode.Singleton.instance;
    }

    private void startNettyClientServer() {
        nettyServerConfig.setListenPort(serverConfig.getNodeClientTcpPort());
        remotingServer = new NettyRemotingServer(nettyServerConfig);
        this.remotingExecutor =
                Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));
        remotingServer.registerDefaultProcessor(new ServerNodeProcessor(this), remotingExecutor);
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

        //heartbeat check with connected instances，remove expired ones
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    ServerNode.this.heartBeatCheck();
                } catch (Exception e) {
                    logger.error("ScheduledTask heartBeatCheck exception", e);
                }
            }
        }, 5000, serverConfig.getHeartBeatCheckInterval(), TimeUnit.MILLISECONDS);
    }


    //todo
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
    }

    @Override
    protected void doStop() {
        this.remotingServer.shutdown();
        this.remotingExecutor.shutdown();
        logger.info("the server controller shutdown OK");
    }

    @Override
    protected void doClose() throws IOException {

    }
}
