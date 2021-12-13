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
    private ServerNode serverNode;
    private RemotingServer remotingServer;

    public ServerToClient(ServerNode serverNode) {
        this.serverNode=serverNode;
    }

    public RemotingCommand callClient(final Channel channel,
                                      final RemotingCommand request
    ) throws RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        if(serverNode!=null&&remotingServer==null){
            remotingServer=serverNode.getRemotingServer();
        }
        if(remotingServer!=null){
            return this.remotingServer.invokeSync(channel, request, 10000);
        }
        return null;
    }

}
