package com.pantheon.server.client;

import com.pantheon.remoting.RemotingServer;
import com.pantheon.remoting.exception.RemotingSendRequestException;
import com.pantheon.remoting.exception.RemotingTimeoutException;
import com.pantheon.remoting.protocol.RemotingCommand;
import com.pantheon.server.ServerNode;
import io.netty.channel.Channel;

/**
 * @author Anthony
 * @create 2021/12/13
 * @desc server to client communication
 */
public class ServerToClient {
    private RemotingServer remotingServer;

    public ServerToClient(ServerNode serverNode) {
        this.remotingServer = serverNode.getRemotingServer();


    }

    public RemotingCommand callClient(final Channel channel,
                                      final RemotingCommand request
    ) throws RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        return this.remotingServer.invokeSync(channel, request, 10000);
    }

}
