package com.pantheon.server.network;

import com.pantheon.common.MessageType;
import com.pantheon.common.ServiceState;
import com.pantheon.server.ServerController;
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
     * todo config
     */
    private static final int DEFAULT_CONNECT_RETRIES = 3;
    /**
     * default connect timeout
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
        new RetryConnectMasterNodeThread().start();
    }

    static class Singleton {
        static ServerNetworkManager instance = new ServerNetworkManager();
    }

    public static ServerNetworkManager getInstance() {
        return Singleton.instance;
    }

    /**
     * 等待重试发起连接的master节点列表
     */
    private CopyOnWriteArrayList<String> retryConnectMasterNodes =
            new CopyOnWriteArrayList<String>();
    /**
     * 跟其他的远程master节点建立好的连接
     */
    private ConcurrentHashMap<Integer, Socket> remoteNodeSockets =
            new ConcurrentHashMap<Integer, Socket>();
    /**
     * 每个节点连接的读写IO线程是否运行的boolean变量
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
     * @param serverNode
     * @return
     */
    public Boolean connectServerNode(String serverNode) {
        boolean fatal = false;

        String[] serverNodeSplited = serverNode.split(":");
        String ip = serverNodeSplited[0];
        int port = Integer.valueOf(serverNodeSplited[1]);

        InetSocketAddress endpoint = new InetSocketAddress(ip, port);

        int retries = 0;

        while (ServerController.getServiceState() == ServiceState.RUNNING &&
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
                addRemoteNodeSocket(remoteServerNode.getNodeId(), socket);
                RemoteServerNodeManager.getInstance().addRemoteServerNode(remoteServerNode);

                LOGGER.info("complete the connection with server node：" + remoteServerNode + "......");

                if (ServerController.isController()) {
//                    AutoRebalanceManager autoRebalanceManager = AutoRebalanceManager.getInstance();
//                    autoRebalanceManager.rebalance(remoteServerNode.getNodeId());
                }

                return true;
            } catch (IOException e) {
                LOGGER.error("exception when connecting with (" + endpoint + ") ！！！");

                retries++;
                if (retries <= DEFAULT_CONNECT_RETRIES) {
                    LOGGER.error("round :" + retries + "retry connect with server node: (" + endpoint + ")......");
                }
            }
        }

        // fatal error
        if (fatal) {
            ServerController.setServiceState(ServiceState.START_FAILED);
            return false;
        }

        // add to retry connect master list
        if (!retryConnectMasterNodes.contains(serverNode)) {
            retryConnectMasterNodes.add(serverNode);
            LOGGER.error("connect to server node (" + serverNode + ") fail, add to retry connect master list......");
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
            outputStream.writeBoolean(ServerController.isController());
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


    public void addRemoteNodeSocket(Integer remoteNodeId, Socket socket) {
        this.remoteNodeSockets.put(remoteNodeId, socket);
    }

    public void remoteRemoteNodeSocket(Integer remoteNodeId) {
        remoteNodeSockets.remove(remoteNodeId);
    }

    /**
     * 添加一个建立好连接的远程master节点
     *
     * @param remoteServerNode
     */
    public void addRemoteServerNode(RemoteServerNode remoteServerNode) {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();
        remoteServerNodeManager.addRemoteServerNode(remoteServerNode);
    }

    /**
     * when {@link CachedPantheonServerConfig#getClusterNodeCount()} in config equals
     * {@link RemoteServerNodeManager#getRemoteServerNodes()}, all servers connected!!!
     */
    public void waitAllServerNodeConnected() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();

        Integer clusterNodeCount = CachedPantheonServerConfig.getInstance().getClusterNodeCount();

        Boolean allServerNodeConnected = false;

        LOGGER.info("waiting connection with all servers......");

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

        LOGGER.info("all servers node connected......");
    }

    /**
     * when {@link CachedPantheonServerConfig#getOtherControllerCandidateServers()} in config equals
     * {@link RemoteServerNodeManager#getOtherControllerCandidates()}, all controller candidate connected!!!
     */
    public void waitAllControllerCandidatesConnected() {
        RemoteServerNodeManager remoteServerNodeManager = RemoteServerNodeManager.getInstance();


        List<String> otherControllerCandidateServers = CachedPantheonServerConfig.getInstance().getOtherControllerCandidateServers();

        LOGGER.info("waiting connection with controller candidates: " + otherControllerCandidateServers + "......");

        while (ServerController.isRunning()) {
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
     * 发送网络请求给远程的其他节点
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
     * 阻塞在这里获取消息
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
     * 重试连接master node的线程
     */
    class RetryConnectMasterNodeThread extends Thread {

        private final Logger LOGGER = LoggerFactory.getLogger(RetryConnectMasterNodeThread.class);

        @Override
        public void run() {
            while (ServerController.isRunning()) {
                // 每隔5分钟运行一次定时重试机制
                List<String> retryConnectSuccessMasterNodes = new ArrayList<String>();

                for (String retryConnectMasterNode : retryConnectMasterNodes) {
                    LOGGER.error("scheduled retry connect master node: " + retryConnectMasterNode + ".......");
                    if (connectServerNode(retryConnectMasterNode)) {
                        retryConnectSuccessMasterNodes.add(retryConnectMasterNode);
                    }
                }

                // 只要重试成功了，就可以把这个节点从定时重试列表里移除就可以了
                for (String retryConnectSuccessMasterNode : retryConnectSuccessMasterNodes) {
                    retryConnectMasterNodes.remove(retryConnectSuccessMasterNode);
                }

                try {
                    Thread.sleep(RETRY_CONNECT_MASTER_NODE_INTERVAL);
                } catch (InterruptedException e) {
                    LOGGER.error("RetryConnectMasterNodeThread is interrupted because of unknown reasons......");
                }
            }
        }
    }

    /**
     * 网络连接监听线程
     */
    class ServerConnectionListener extends Thread {

        private final Logger LOGGER = LoggerFactory.getLogger(ServerConnectionListener.class);

        /**
         * 默认的监听端口号的重试次数
         */
        public static final int DEFAULT_RETRIES = 3;

        /**
         * 网络连接监听服务器
         */
        private ServerSocket serverSocket;
        /**
         * 当前已经尝试重试监听端口号的次数
         */
        private int retries = 0;

        /**
         * 线程的运行逻辑
         */
        @Override
        public void run() {

            // 在线程运行期间是否遇到了出乎意料之外的崩溃异常
            boolean fatal = false;

            // 只要系统还在运行，而且监听端口号的重试次数小于默认重试次数
            while (ServerController.isRunning()
                    && retries <= DEFAULT_RETRIES) {
                try {
                    // 获取master节点内部网络通信的端口号
                    int port = CachedPantheonServerConfig.getInstance().getNodeInternTcpPort();
                    InetSocketAddress endpoint = new InetSocketAddress(port);

                    // 基于ServerSocket监听master节点内部网络通信的端口号
                    this.serverSocket = new ServerSocket();
                    this.serverSocket.setReuseAddress(true);
                    this.serverSocket.bind(endpoint);

                    LOGGER.info("server连接请求线程，已经绑定端口号: " + port + "，等待监听连接请求......");

                    // 跟发起连接请求的master建立网络连接
                    while (ServerController.getServiceState() == ServiceState.RUNNING) {
                        // id比自己大的master节点发送网络连接请求过来
                        // BIO，建立网路连接
                        Socket socket = this.serverSocket.accept();
                        socket.setTcpNoDelay(true); // 网络通信不允许延迟
                        socket.setSoTimeout(0); // 读取数据时的超时时间为0，没有超时，阻塞读取

                        // 读取对方传输过来的信息
                        RemoteServerNode remoteServerNode = readRemoteNodeInformation(socket);
                        if (remoteServerNode == null) {
                            fatal = true;
                            break;
                        }

                        // 为建立好的网络连接，启动IO线程
                        startServerIOThreads(remoteServerNode.getNodeId(), socket);
                        // 维护这个建立成功的连接
                        addRemoteNodeSocket(remoteServerNode.getNodeId(), socket);
                        // 添加建立连接的远程节点
                        addRemoteServerNode(remoteServerNode);

                        // 发送自己的信息过去给对方
                        if (!sendSelfInformation(socket)) {
                            fatal = true;
                            break;
                        }

                        if (ServerController.isController()) {
//                            AutoRebalanceManager autoRebalanceManager = AutoRebalanceManager.getInstance();
//                            autoRebalanceManager.rebalance(remoteServerNode.getNodeId());
                        }

                        LOGGER.info("连接监听线程已经跟远程server节点建立连接：" + remoteServerNode + ", IO线程全部启动......");
                    }
                } catch (IOException e) {
                    LOGGER.error("连接监听线程在监听连接请求的过程中发生异常！！！", e);

                    // 重试次数加1
                    this.retries++;
                    if (this.retries <= DEFAULT_RETRIES) {
                        LOGGER.error("本次是第" + retries + "次重试去监听连接请求......");
                    }
                } finally {
                    // 将ServerSocket进行关闭
                    try {
                        this.serverSocket.close();
                    } catch (IOException ex) {
                        LOGGER.error("关闭ServerSocket异常！！！", ex);
                    }
                }

                // 如果是遇到了系统不可逆的异常，直接崩溃
                if (fatal) {
                    break;
                }
            }

            // 在这里，就说明这个master节点无法监听其他节点的连接请求
            ServerController.setServiceState(ServiceState.START_FAILED);

            LOGGER.error("无法正常监听其他server节点的连接请求，系统即将崩溃！！！");
        }

    }

    /**
     * 删除跟指定节点的网络连接数据
     *
     * @param remoteNodeId
     */
    public void clearConnection(Integer remoteNodeId) {
        LOGGER.info("跟节点【" + remoteNodeId + "】的网络连接断开，清理相关数据......");

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
