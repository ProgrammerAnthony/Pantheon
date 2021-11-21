package com.pantheon.server.network;

import com.pantheon.server.ServerController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Socket写IO线程
 */
public class ServerWriteIOThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerWriteIOThread.class);

    public static final Integer TERMINATE_MESSAGE_CAPACITY = 4;

    /**
     * 远程节点id
     */
    private Integer remoteNodeId;
    /**
     * master节点之间的网络连接
     */
    private Socket socket;
    /**
     * 针对远程节点的输出流
     */
    private DataOutputStream outputStream;
    /**
     * 发送消息队列
     */
    private LinkedBlockingQueue<ByteBuffer> sendQueue;
    /**
     * 线程运行信号量
     */
    private IOThreadRunningSignal ioThreadRunningSignal;

    /**
     * 构造函数
     * @param socket
     */
    public ServerWriteIOThread(
            Integer remoteNodeId,
            Socket socket,
            LinkedBlockingQueue<ByteBuffer> sendQueue,
            IOThreadRunningSignal ioThreadRunningSignal) {
        this.remoteNodeId = remoteNodeId;
        this.socket = socket;
        try {
            this.outputStream = new DataOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            LOGGER.error("get data output stream from socket error......", e);
        }
        this.sendQueue = sendQueue;
        this.ioThreadRunningSignal = ioThreadRunningSignal;
    }

    /**
     * 线程运行逻辑
     */
    @Override
    public void run() {
        while(ServerController.isRunning() && ioThreadRunningSignal.isRunning()) {
            try {
                // 阻塞式获取待发送的消息
                ByteBuffer message = sendQueue.take();

                // 判断一下当前线程是否收到了一个终止运行的消息
                if(message.capacity() == TERMINATE_MESSAGE_CAPACITY) {
                    continue;
                }

                // 通过IO流把消息发送给远程节点
                outputStream.writeInt(message.capacity());
                outputStream.write(message.array());
                outputStream.flush();
            } catch (InterruptedException e) {
                LOGGER.error("get message from send queue error......", e);
//                NodeStatus.fatal();
            } catch (IOException e) {
                LOGGER.error("send message to remote node error: " + socket.getRemoteSocketAddress());
//                NodeStatus.fatal();
            }
        }

        LOGGER.info("跟节点【" + remoteNodeId + "】的网络连接的写IO线程，即将终止运行......");
//        if(NodeStatus.isFatal()) {
//            LOGGER.error("跟节点【" + remoteNodeId + "】的网络连接的写IO线程，遇到不可逆转的重大事故，系统即将崩溃......");
//        }
    }

}
