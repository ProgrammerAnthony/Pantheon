package com.pantheon.server.network;

import com.pantheon.common.component.Lifecycle;
import com.pantheon.server.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;


public class ServerReadIOThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerReadIOThread.class);

    /**
     * remote node id
     */
    private Integer remoteNodeId;
    /**
     * master connections
     */
    private Socket socket;
    /**
     * inputStream with master nodes
     */
    private DataInputStream inputStream;
    /**
     * receive queue
     */
    private LinkedBlockingQueue<ByteBuffer> receiveQueue;
    /**
     * server network manager
     */
    private ServerNetworkManager serverNetworkManager;
    /**
     * thread is running or not
     */
    private IOThreadRunningSignal ioThreadRunningSignal;


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


    @Override
    public void run() {
        ServerNode serverNode = ServerNode.getInstance();
        while ((serverNode.lifecycleState().equals(Lifecycle.State.INITIALIZED)
                || serverNode.lifecycleState().equals(Lifecycle.State.STARTED)) && ioThreadRunningSignal.isRunning()) {
            try {
                // blocking read from DataInputStream
                int messageLength = inputStream.readInt();
                byte[] messageBytes = new byte[messageLength];
                inputStream.readFully(messageBytes, 0, messageLength);

                // bytes to ByteBuffer
                ByteBuffer message = ByteBuffer.wrap(messageBytes);

                receiveQueue.put(message);
            } catch (EOFException e) {
                LOGGER.error("connection broker with 【" + remoteNodeId + "】......", e);
                serverNetworkManager.clearConnection(remoteNodeId);
//                HighAvailabilityManager highAvailabilityManager = HighAvailabilityManager.getInstance();
//                highAvailabilityManager.handleDisconnectedException(remoteNodeId);
            } catch (IOException e) {
                LOGGER.error("IOException when connection with node id: 【" + remoteNodeId + "】......", e);
                serverNode.stop();
            } catch (InterruptedException e) {
                LOGGER.error("InterruptedException when connection with node id: 【" + remoteNodeId + "】......", e);
                serverNode.stop();
            }
        }

        LOGGER.info("read connection with【" + remoteNodeId + "】will finish......");
        if (serverNode.lifecycleState().equals(Lifecycle.State.STOPPED)) {
            LOGGER.error("read connection with【" + remoteNodeId + "】collapsed......");
        }
    }

}
