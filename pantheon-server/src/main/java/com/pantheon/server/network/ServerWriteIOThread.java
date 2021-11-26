package com.pantheon.server.network;

import com.pantheon.common.component.Lifecycle;
import com.pantheon.server.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;


public class ServerWriteIOThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerWriteIOThread.class);

    public static final Integer TERMINATE_MESSAGE_CAPACITY = 4;

    /**
     * remote node id
     */
    private Integer remoteNodeId;
    /**
     * master connections
     */
    private Socket socket;
    /**
     * outputStream for remote node
     */
    private DataOutputStream outputStream;
    /**
     * send queue
     */
    private LinkedBlockingQueue<ByteBuffer> sendQueue;
    /**
     * thread is running
     */
    private IOThreadRunningSignal ioThreadRunningSignal;

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


    @Override
    public void run() {
        ServerNode serverNode = ServerNode.getInstance();
        while ((serverNode.lifecycleState().equals(Lifecycle.State.INITIALIZED)
                || serverNode.lifecycleState().equals(Lifecycle.State.STARTED)) && ioThreadRunningSignal.isRunning()) {
            try {
                // blocking take message
                ByteBuffer message = sendQueue.take();

                // receive terminate message
                if (message.capacity() == TERMINATE_MESSAGE_CAPACITY) {
                    continue;
                }
                outputStream.writeInt(message.capacity());
                outputStream.write(message.array());
                outputStream.flush();
            } catch (InterruptedException e) {
                LOGGER.error("get message from send queue error......", e);
                serverNode.stop();
            } catch (IOException e) {
                LOGGER.error("send message to remote node error: " + socket.getRemoteSocketAddress());
                serverNode.stop();
            }
        }

        LOGGER.info("write connection with【" + remoteNodeId + "】will finish......");
        if (serverNode.lifecycleState().equals(Lifecycle.State.STOPPED)) {
            LOGGER.error("write connection with【" + remoteNodeId + "】collapsed......");
        }
    }

}
