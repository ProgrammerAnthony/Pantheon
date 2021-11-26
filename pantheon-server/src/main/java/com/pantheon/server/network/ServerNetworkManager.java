package com.pantheon.server.network;

import com.pantheon.common.MessageType;
import com.pantheon.common.ThreadFactoryImpl;
import com.pantheon.common.component.Lifecycle;
import com.pantheon.server.ServerNode;
import com.pantheon.server.config.CachedPantheonServerConfig;
import com.pantheon.server.node.RemoteServerNode;
import com.pantheon.server.node.RemoteServerNodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 集群内部的master节点之间进行网络通信的管理组件
 * <p>
 * 1、跟其他的master节点建立网络连接，避免出现重复的连接
 * 2、在底层基于队列和线程，帮助我们发送请求给其他的机器节点
 * 3、同上，需要接受其他节点发送过来的请求，交给我们来进行业务逻辑上的处理
 */
public class ServerNetworkManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerNetworkManager.class);

    /**
     * default retry connect times
     * todo config-able
     */
    private static final int DEFAULT_CONNECT_RETRIES = 3;
    /**
     * default connect timeout
     * todo config-able
     */
    private static final int CONNECT_TIMEOUT = 5000;
    /**
     * interval for retrying connect with master
     */
    private static final long RETRY_CONNECT_MASTER_NODE_INTERVAL = 1 * 60 * 1000;
    /**
     * interval for checking all other nodes connection
     */
    private static final long CHECK_ALL_OTHER_NODES_CONNECT_INTERVAL = 10 * 1000;
    /**
     * interval for waiting all other master nodes connection
     */
    private static final Long ALL_MASTER_NODE_CONNECT_CHECK_INTERVAL = 100L;

    private ServerNetworkManager() {
        ScheduledThreadPoolExecutor scheduledExecutorService = new ScheduledThreadPoolExecutor(2,
                new ThreadFactoryImpl("RetryConnectMasterNodeThread")
        );
        scheduledExecutorService.scheduleAtFixedRate(new RetryConnectMasterNodeThread(), 10, RETRY_CONNECT_MASTER_NODE_INTERVAL, TimeUnit.MILLISECONDS);
    }

    static class Singleton {
        static ServerNetworkManager instance = new ServerNetworkManager();
    }

    public static ServerNetworkManager getInstance() {
        return Singleton.instance;
    }

    /**
     * nodes waiting for retrying
     */
    private CopyOnWriteArrayList<String> retryConnectMasterNodes =
            new CopyOnWriteArrayList<String>();
    /**
     * connection with other master nodes
     */
    private ConcurrentHashMap<Integer, Socket> remoteNodeSockets =
            new ConcurrentHashMap<Integer, Socket>();
    /**
     * read & write thread is running or not
     */
    private ConcurrentHashMap<Integer, IOThreadRunningSignal> ioThreadRunningSignals =
            new ConcurrentHashMap<>();
    /**
     * send messages queue
     */
    private ConcurrentHashMap<Integer/*nodeId*/, LinkedBlockingQueue<ByteBuffer>> sendQueues =
            new ConcurrentHashMap<Integer, LinkedBlockingQueue<ByteBuffer>>();
    /**
     * receive messages queue
     */
    private LinkedBlockingQueue<ByteBuffer> receiveQueue =
            new LinkedBlockingQueue<ByteBuffer>();

    /**
     * connect server listener
     */
    public void startServerConnectionListener() {
        new ServerConnectionListener().start();
    }

    /**
     * connect to all controller candidate servers
     *
     * @return
     */
    public Boolean connectAllControllerCandidates() {
        String controllerCandidateServers = CachedPantheonServerConfig.getInstance().getControllerCandidateServers();
        String[] controllerCandidateServersSplited = controllerCandidateServers.split(",");

        for (String controllerCandidateServer : controllerCandidateServersSplited) {
            if (!connectServerNode(controllerCandidateServer)) {
                continue;
            }
        }

        return true;
    }

    /**
     * connect before candidates avoid of nodes' duplicate connection
     *
     * @return
     */
    public Boolean connectBeforeControllerCandidateServers() {

        List<String> beforeControllerCandidateServers =
                CachedPantheonServerConfig.getInstance().getBeforeControllerCandidateServers();
        if (beforeControllerCandidateServers.size() == 0) {
            return true;
        }

        for (String beforeControllerCandidateServer : beforeControllerCandidateServers) {
            if (!connectServerNode(beforeControllerCandidateServer)) {
                continue;
            }
        }

        return true;
    }

    /**
     * connect server node
     *
     * @param serverNodeStr
     * @return
     */
    public Boolean connectServerNode(String serverNodeStr) {
        boolean fatal = false;

        String[] serverNodeSplited = serverNodeStr.split(":");
        String ip = serverNodeSplited[0];
        int port = Integer.valueOf(serverNodeSplited[1]);

        InetSocketAddress endpoint = new InetSocketAddress(ip, port);

        int retries = 0;
        ServerNode serverNode = ServerNode.getInstance();
        while (serverNode.lifecycleState().equals(Lifecycle.State.INITIALIZED) &&
                retries <= DEFAULT_CONNECT_RETRIES) {
            try {
                Socket socket = new Socket();
                socket.setTcpNoDelay(true);
                socket.setSoTimeout(0);
                socket.connect(endpoint, CONNECT_TIMEOUT);

                if (!sendSelfInformation(socket)) {
                    fatal = true;
                    break;
                }
                RemoteServerNode remoteServerNode = readRemoteNodeInformation(socket);
                if (remoteServerNode == null) {
                    fatal = true;
                    break;
                }

                startServerIOThreads(remoteServerNode.getNodeId(), socket);
                this.remoteNodeSockets.put(remoteServerNode.getNodeId(), socket);
                RemoteServerNodeManager.getInstance().addRemoteServerNode(remoteServerNode);

                LOGGER.info("complete the connection with server node：" + remoteServerNode + "......");

                if (ServerNode.isController()) {
//                    AutoRebalanceManager autoRebalanceManager = AutoRebalanceManager.getInstance();
//                    autoRebalanceManager.rebalance(remoteServerNode.getNodeId());
                }

                return true;
            } catch (IOException e) {
                LOGGER.error("exception when connecting with (" + endpoint + ") ！！！");
                e.printStackTrace();
                retries++;
                if (retries <= DEFAULT_CONNECT_RETRIES) {
                    LOGGER.error("round :" + retries + " retry connect with server node: (" + endpoint + ")......");
                }
            }
        }

        // fatal error
        if (fatal) {
            serverNode.stop();
            return false;
        }

        // add to retry connect master list
        if (!retryConnectMasterNodes.contains(serverNodeStr)) {
            retryConnectMasterNodes.add(serverNodeStr);
            LOGGER.error("connect to server node (" + serverNodeStr + ") fail, add to retry connect master list......");
        }

        return false;
    }


    public boolean sendSelfInformation(Socket socket) {

        Integer nodeId = CachedPantheonServerConfig.getInstance().getNodeId();
        Boolean isControllerCandidate = CachedPantheonServerConfig.getInstance().isControllerCandidate();
        String ip = CachedPantheonServerConfig.getInstance().getNodeIp();
        Integer clientTcpPort = CachedPantheonServerConfig.getInstance().getNodeClientTcpPort();

        DataOutputStream outputStream = null;
        try {
            outputStream = new DataOutputStream(socket.getOutputStream());
            outputStream.writeInt(nodeId);
            outputStream.writeBoolean(isControllerCandidate);
            outputStream.writeInt(ip.length());
            outputStream.write(ip.getBytes());
            outputStream.writeInt(clientTcpPort);
            outputStream.writeBoolean(ServerNode.isController());
            outputStream.flush();
        } catch (IOException e) {
            LOGGER.error("exception occurs when connect with server node！！！", e);

            try {
                socket.close();
            } catch (IOException ex) {
                LOGGER.error("close socket with exception！！！", ex);
            }

            return false;
        }

        return true;
    }

    /**
     * blocking read information from other nodes
     *
     * @param socket
     * @return
     */
    public RemoteServerNode readRemoteNodeInformation(Socket socket) {
        try {
            DataInputStream inputStream = new DataInputStream(socket.getInputStream());

            Integer remoteNodeId = inputStream.readInt();
            Boolean isControllerCandidate = inputStream.readBoolean();

            Integer ipLength = inputStream.readInt();
            byte[] ipBytes = new byte[ipLength];
            inputStream.read(ipBytes);
            String ip = new String(ipBytes);

            Integer clientPort = inputStream.readInt();
            boolean isController = inputStream.readBoolean();

            RemoteServerNode remoteServerNode = new RemoteServerNode(
                    remoteNodeId,
                    isControllerCandidate,
                    ip,
                    clientPort,
                    isController
            );

            return remoteServerNode;
        } catch (IOException e) {
            LOGGER.error("read information from other server node exception！！！", e);

            try {
                socket.close();
            } catch (IOException ex) {
                LOGGER.error("Socket close exception！！！", ex);
            }
        }
        return null;
    }

    /**
     * background write and read thread after connecting
     *
     * @param socket
     */
    public void startServerIOThreads(Integer remoteNodeId, Socket socket) {
        // init send message queue
        LinkedBlockingQueue<ByteBuffer> sendQueue =
                new LinkedBlockingQueue<ByteBuffer>();
        sendQueues.put(remoteNodeId, sendQueue);

        IOThreadRunningSignal ioThreadRunningSignal = new IOThreadRunningSignal(true);
        ioThreadRunningSignals.put(remoteNodeId, ioThreadRunningSignal);

        new ServerWriteIOThread(remoteNodeId, socket, sendQueue, ioThreadRunningSignal).start();
        new ServerReadIOThread(remoteNodeId, socket, receiveQueue, this, ioThreadRunningSignal).start();
    }

    public void shutdownIOThread(Integer remoteNodeId) {
        ioThreadRunningSignals.get(remoteNodeId).setIsRunning(false);
        ioThreadRunningSignals.remove(remoteNodeId);
        sendQueues.remove(remoteNodeId);
    }


    public void remoteRemoteNodeSocket(Integer remoteNodeId) {
        remoteNodeSockets.remove(remoteNodeId);
    }


    /**
     * when {@link CachedPantheonServerConfig#getClusterNodeCount()} in config equals
     * {@link RemoteServerNodeManager#getRemoteServerNodes()}, all servers connected!!!
     */
    public void waitAllServerNodeConnected() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();

        Integer clusterNodeCount = CachedPantheonServerConfig.getInstance().getClusterNodeCount();

        Boolean allServerNodeConnected = false;

        LOGGER.info("waiting connection with all of {} servers......", clusterNodeCount);

        while (!allServerNodeConnected) {
            try {
                Thread.sleep(ALL_MASTER_NODE_CONNECT_CHECK_INTERVAL);
            } catch (InterruptedException e) {
                LOGGER.error("InterruptedException！！！", e);
            }
            if (clusterNodeCount == remoteServerNodeManager.getRemoteServerNodes().size() + 1) {
                allServerNodeConnected = true;
            }
        }

        LOGGER.info("all servers node connected!!!!");
    }

    /**
     * when {@link CachedPantheonServerConfig#getOtherControllerCandidateServers()} in config equals
     * {@link RemoteServerNodeManager#getOtherControllerCandidates()}, all controller candidate connected!!!
     */
    public void waitAllControllerCandidatesConnected() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();


        List<String> otherControllerCandidateServers = CachedPantheonServerConfig.getInstance().getOtherControllerCandidateServers();

        LOGGER.info("waiting connection with controller candidates: " + otherControllerCandidateServers + "......");
        ServerNode serverNode = ServerNode.getInstance();
        while (serverNode.lifecycleState().equals(Lifecycle.State.INITIALIZED)) {
            boolean allControllerCandidatesConnected = false;

            List<RemoteServerNode> connectedControllerCandidates =
                    remoteServerNodeManager.getOtherControllerCandidates();
            if (connectedControllerCandidates.size() == otherControllerCandidateServers.size()) {
                allControllerCandidatesConnected = true;
            }

            if (allControllerCandidatesConnected) {
                LOGGER.info(" all controller candidate connected!!!");
                break;
            }

            try {
                Thread.sleep(CHECK_ALL_OTHER_NODES_CONNECT_INTERVAL);
            } catch (InterruptedException e) {
                LOGGER.error("InterruptedException！！！", e);
            }
        }
    }

    /**
     * send message to other nodes
     *
     * @param remoteNodeId
     * @param message
     * @return
     */
    public Boolean sendMessage(Integer remoteNodeId, ByteBuffer message) {
        try {
            LinkedBlockingQueue<ByteBuffer> sendQueue = sendQueues.get(remoteNodeId);
            if (sendQueue != null) {
                sendQueue.put(message);
            }
        } catch (InterruptedException e) {
            LOGGER.error("put message into send queue error, remoteNodeId=" + remoteNodeId, e);
            return false;
        }
        return true;
    }

    /**
     * blocking take
     *
     * @return
     */
    public ByteBuffer takeMessage() {
        try {
            return receiveQueue.take();
        } catch (Exception e) {
            LOGGER.error("take message from receive queue error......", e);
        }
        return null;
    }

    /**
     * retry connection with master node
     */
    class RetryConnectMasterNodeThread implements Runnable {

        private final Logger LOGGER = LoggerFactory.getLogger(RetryConnectMasterNodeThread.class);

        @Override
        public void run() {
            ServerNode serverNode = ServerNode.getInstance();
            while (serverNode.lifecycleState().equals(Lifecycle.State.INITIALIZED) || serverNode.lifecycleState().equals(Lifecycle.State.STARTED)) {

                List<String> retryConnectSuccessMasterNodes = new ArrayList<String>();

                for (String retryConnectMasterNode : retryConnectMasterNodes) {
                    LOGGER.error("scheduled retry connect master node: " + retryConnectMasterNode + ".......");
                    if (connectServerNode(retryConnectMasterNode)) {
                        retryConnectSuccessMasterNodes.add(retryConnectMasterNode);
                    }
                }

                //remove from retry list after success
                for (String retryConnectSuccessMasterNode : retryConnectSuccessMasterNodes) {
                    retryConnectMasterNodes.remove(retryConnectSuccessMasterNode);
                }
            }
        }
    }

    /**
     * server connection listener
     */
    class ServerConnectionListener extends Thread {

        private final Logger LOGGER = LoggerFactory.getLogger(ServerConnectionListener.class);

        /**
         * default retry times
         */
        public static final int DEFAULT_RETRIES = 3;


        private ServerSocket serverSocket;
        /**
         * retried times
         */
        private int retries = 0;


        @Override
        public void run() {


            boolean fatal = false;

            ServerNode serverNode = ServerNode.getInstance();
            while (serverNode.lifecycleState().equals(Lifecycle.State.INITIALIZED)
                    && retries <= DEFAULT_RETRIES) {
                try {
                    // connect using intern tcp port
                    int port = CachedPantheonServerConfig.getInstance().getNodeInternTcpPort();
                    InetSocketAddress endpoint = new InetSocketAddress(port);
                    this.serverSocket = new ServerSocket();
                    this.serverSocket.setReuseAddress(true);
                    this.serverSocket.bind(endpoint);

                    LOGGER.info("server connection thread bind to: " + port + "，waiting for connection......");

                    while (serverNode.lifecycleState().equals(Lifecycle.State.INITIALIZED)
                            || serverNode.lifecycleState().equals(Lifecycle.State.STARTED)) {
                        // BIO
                        Socket socket = this.serverSocket.accept();
                        socket.setTcpNoDelay(true);
                        socket.setSoTimeout(0);

                        RemoteServerNode remoteServerNode = readRemoteNodeInformation(socket);
                        if (remoteServerNode == null) {
                            fatal = true;
                            break;
                        }
                        startServerIOThreads(remoteServerNode.getNodeId(), socket);
                        ServerNetworkManager.this.remoteNodeSockets.put(remoteServerNode.getNodeId(), socket);
                        RemoteServerNodeManager.getInstance().addRemoteServerNode(remoteServerNode);

                        // send self information
                        if (!sendSelfInformation(socket)) {
                            fatal = true;
                            break;
                        }

                        if (ServerNode.isController()) {
//                            AutoRebalanceManager autoRebalanceManager = AutoRebalanceManager.getInstance();
//                            autoRebalanceManager.rebalance(remoteServerNode.getNodeId());
                        }

                        LOGGER.info("connected with server node：" + remoteServerNode + ", IO thread started......");
                    }
                } catch (IOException e) {
                    LOGGER.error("IOException when monitor connecting！！！", e);

                    this.retries++;
                    if (this.retries <= DEFAULT_RETRIES) {
                        LOGGER.error("retry times:" + retries + "for monitoring connecting......");
                    }
                } finally {
                    // close ServerSocket
                    try {
                        this.serverSocket.close();
                    } catch (IOException ex) {
                        LOGGER.error("IOException when close socket！！！", ex);
                    }
                }

                if (fatal) {
                    break;
                }
            }

            serverNode.stop();

            LOGGER.error("unable to monitor other servers' connection！！！");
        }

    }


    public void clearConnection(Integer remoteNodeId) {
        LOGGER.info("broker connection with【" + remoteNodeId + "】，cleaning......");

        remoteNodeSockets.remove(remoteNodeId);
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        remoteServerNodeManager.removeServerNode(remoteNodeId);

        IOThreadRunningSignal ioThreadRunningSignal =
                ioThreadRunningSignals.get(remoteNodeId);
        ioThreadRunningSignal.setIsRunning(false);

        ByteBuffer terminateBuffer = ByteBuffer.allocate(4);
        terminateBuffer.putInt(MessageType.TERMINATE);
        sendQueues.get(remoteNodeId).offer(terminateBuffer);

        sendQueues.remove(remoteNodeId);
    }

}
