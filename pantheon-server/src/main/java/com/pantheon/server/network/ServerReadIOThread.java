package com.pantheon.server.network;

import com.pantheon.server.ServerController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Socket读IO线程
 */
public class ServerReadIOThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerReadIOThread.class);

    /**
     * 远程节点id
     */
    private Integer remoteNodeId;
    /**
     * master节点间的网络连接
     */
    private Socket socket;
    /**
     * 从远程节点读取数据的输入流
     */
    private DataInputStream inputStream;
    /**
     * 消息接收队列
     */
    private LinkedBlockingQueue<ByteBuffer> receiveQueue;
    /**
     * 服务端网络管理组件
     */
    private ServerNetworkManager serverNetworkManager;
    /**
     * 线程运行信号量
     */
    private IOThreadRunningSignal ioThreadRunningSignal;

    /**
     * 构造函数
     * @param socket
     */
    public ServerReadIOThread(
            Integer remoteNodeId,
            Socket socket,
            LinkedBlockingQueue<ByteBuffer> receiveQueue,
            ServerNetworkManager serverNetworkManager,
            IOThreadRunningSignal ioThreadRunningSignal) {
        this.remoteNodeId = remoteNodeId;
        this.socket = socket;
        try {
            this.inputStream = new DataInputStream(socket.getInputStream());
            this.receiveQueue = receiveQueue;
        } catch (IOException e) {
            LOGGER.error("get input stream from socket error......", e);
        }
        this.serverNetworkManager = serverNetworkManager;
        this.ioThreadRunningSignal = ioThreadRunningSignal;
    }

    /**
     * 线程运行逻辑
     */
    @Override
    public void run() {
        while(ServerController.isRunning() && ioThreadRunningSignal.isRunning()) {
            try {
                // 从IO流里读取一条消息
                int messageLength = inputStream.readInt();
                byte[] messageBytes = new byte[messageLength];
                inputStream.readFully(messageBytes, 0, messageLength);

                // 将消息字节数组封装成ByteBuffer
                ByteBuffer message = ByteBuffer.wrap(messageBytes);

                // 将消息放入接收队列里去
                receiveQueue.put(message);
            } catch(EOFException e) {
                LOGGER.error("跟远程节点【" + remoteNodeId + "】之间的网络连接突然断开......", e);
                // 删除断开连接的节点的网络连接数据
                serverNetworkManager.clearConnection(remoteNodeId);
                // 跟其他节点的网络连接断开，把异常交给高可用组件去处理
//                HighAvailabilityManager highAvailabilityManager = HighAvailabilityManager.getInstance();
//                highAvailabilityManager.handleDisconnectedException(remoteNodeId);
            } catch (IOException e) {
                LOGGER.error("在使用输入流从远程节点【" + remoteNodeId + "】读取数据时，发生未知的IO异常......", e);
//                NodeStatus.fatal();
            } catch (InterruptedException e) {
                LOGGER.error("跟节点【" + remoteNodeId + "】的网络连接读取io线程发生中断异常......", e);
//                NodeStatus.fatal();
            }
        }

        LOGGER.info("跟节点【" + remoteNodeId + "】的网络连接的读IO线程，即将终止运行......");
//        if(NodeStatus.isFatal()) {
//            LOGGER.error("跟节点【" + remoteNodeId+ "】的网络连接的写IO线程，遇到不可逆转的重大事故，系统即将崩溃......");
//        }
    }

}
