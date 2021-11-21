package com.pantheon.server;

import com.pantheon.common.ServerNodeRole;
import com.pantheon.common.ServiceState;
import com.pantheon.common.ThreadFactoryImpl;
import com.pantheon.common.exception.InitException;
import com.pantheon.remoting.RemotingServer;
import com.pantheon.remoting.netty.NettyRemotingServer;
import com.pantheon.remoting.netty.NettyServerConfig;
import com.pantheon.server.config.PantheonServerConfig;
import com.pantheon.server.network.ServerMessageReceiver;
import com.pantheon.server.network.ServerNetworkManager;
import com.pantheon.server.node.*;
import com.pantheon.server.processor.ServerNodeProcessor;
import com.pantheon.server.slot.SlotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Anthony
 * @create 2021/11/18
 * @desc 1 todo build BIO for server inside communication
 * 2 todo rethink and build the mechanism of client and server RocketMq
 * 3 todo build heartbeat mechanism of client and server
 * 4 todo design the message protocol
 * 5 todo design slots mechanism and treat it as topic in RocketMq
 **/
public class ServerController {
    private NettyServerConfig nettyServerConfig;
    private PantheonServerConfig serverConfig;
    private RemotingServer remotingServer;
    private ExecutorService remotingExecutor;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "ServerControllerScheduledThread"));
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);
    private static ServiceState serviceState = ServiceState.CREATE_JUST;
    private static ServerNodeRole serverNodeRole = ServerNodeRole.COMMON_NODE;


    public ServerController(PantheonServerConfig serverConfig, NettyServerConfig nettyServerConfig) {
        this.nettyServerConfig = nettyServerConfig;
        this.serverConfig = serverConfig;
    }

    public boolean initialize() {
        synchronized (ServerController.class) {
            switch (serviceState) {
                case CREATE_JUST:
                    serviceState = ServiceState.RUNNING;
                    logger.info("server with id :{}, listen to tcp on port :{}", serverConfig.getNodeId(), serverConfig.getNodeClientTcpPort());
                    nettyServerConfig.setListenPort(serverConfig.getNodeClientTcpPort());
                    remotingServer = new NettyRemotingServer(nettyServerConfig);
                    this.remotingExecutor =
                            Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));
                    remotingServer.registerDefaultProcessor(new ServerNodeProcessor(this), remotingExecutor);
                    remotingServer.start();


                    this.startScheduledTask();

                    Boolean isControllerCandidate = serverConfig.isControllerCandidate();
                    //listener for other servers' connection
                    ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
                    serverNetworkManager.startServerConnectionListener();

                    // controller candidate
                    if (isControllerCandidate) {
                        // connect before candidates (not all) avoid of nodes' duplicate connection
                        if (!serverNetworkManager.connectBeforeControllerCandidateServers()) {
                            serviceState = ServiceState.START_FAILED;
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


                    Boolean isController = false;
                    ServerNodeRole serverNodeRole = ServerNodeRole.COMMON_NODE;

                    if (isControllerCandidate) {
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
                            logger.info("after election, the role of server node：" + (serverNodeRole == ServerNodeRole.CONTROLLER_NODE ? "Controller" : "Controller candidate"));

                            if (serverNodeRole == ServerNodeRole.CONTROLLER_NODE) {
                                //controller determine slots allocation
                                isController = true;
                                Controller controller = Controller.getInstance();
                                controller.allocateSlots();
                                controller.initControllerNode();
                                controller.sendControllerNodeId();
                            } else if (serverNodeRole == ServerNodeRole.CONTROLLER_CANDIDATE_NODE) {
                                //controller candidate wait for slots allocation
                                controllerCandidate.waitForSlotsAllocation();
                                controllerCandidate.waitForSlotsReplicaAllocation();
                                controllerCandidate.waitReplicaNodeIds();
                            }
                        }
                    }

                    //master node (not controller) wait for slots data
                    if (!isController) {
                        SlotManager slotManager = SlotManager.getInstance();
                        slotManager.initSlots(null);
                        slotManager.initSlotsReplicas(null, false);
                        slotManager.initReplicaNodeId(null);
                        ControllerNode.setNodeId(serverMessageReceiver.takeControllerNodeId());
                    }

                    // 如果是重新加入集群的话

                    // 申请controller发起数据rebalance，把一些数据均匀的分配给他
                    // rebalance的算法，可以做的很复杂，也可以做的比较简单一些
                    // 你要是加入一些新的节点这样子的话，按理来说应该按照他之前的那种槽位计算的方式，重新计算一遍
                    // 重新计算副本节点id，重新计算槽位副本
                    // 必然会保证非常的均匀和均衡，就需要把一些slots开始来做转移，每个slots里都有自己的数据
                    // 副本得重新进行初始化
                    // 就会导致集群里有大量的数据在进行移动，各个节点之间突然可能会出现大量的数据在转移
                    // 第一个版本的rebalance的算法，可以做的简单一些
                    // 你发起一个请求给controller，让controller给你适当的分配一些slots
                    // slots副本也可以重新进行一分配

                    // 把一个叫做雏形的版本给他先实现一下
                    // 我们应该是说让我们新加入集群的节点主动发送请求给controller去做rebalance
                    // 让controller主动感知到集群里新加入了节点，自动去做rebalance


                    setServerRole(serverNodeRole);

                    break;
                case START_FAILED:
                    throw new InitException("ServerBootstrap failed to initialize ");
                default:
                    break;
            }
        }
        return true;
    }

    //todo startScheduledTask here
    private void startScheduledTask() {

        //heartbeat check with connected instances，remove expired ones
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    ServerController.this.heartBeatCheck();
                } catch (Exception e) {
                    logger.error("ScheduledTask heartBeatCheck exception", e);
                }
            }
        }, 5000, serverConfig.getHeartBeatCheckInterval(), TimeUnit.MILLISECONDS);
    }


    private void heartBeatCheck() {

    }


    public void shutdown() {
        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    break;
                case RUNNING:
                    this.remotingServer.shutdown();
                    this.remotingExecutor.shutdown();
                    logger.info("the server controller shutdown OK");
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                default:
                    break;
            }
        }
    }

    public PantheonServerConfig getServerConfig() {
        return serverConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }


    public static boolean isRunning() {
        return getServiceState() == ServiceState.RUNNING;
    }

    public static boolean isController() {
        return getServerNodeRole() == ServerNodeRole.CONTROLLER_NODE;
    }

    /**
     * global state of current server
     *
     * @return
     */
    public static ServiceState getServiceState() {
        return serviceState;
    }

    public static synchronized void setServiceState(ServiceState serviceState) {
        ServerController.serviceState = serviceState;
    }

    public static synchronized ServerNodeRole setServerRole(ServerNodeRole serverNodeRole) {
        ServerController.serverNodeRole = serverNodeRole;
        return ServerController.serverNodeRole;
    }

    public static ServerNodeRole getServerNodeRole() {
        return serverNodeRole;
    }
}
