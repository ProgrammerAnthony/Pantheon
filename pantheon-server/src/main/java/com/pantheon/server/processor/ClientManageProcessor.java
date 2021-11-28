package com.pantheon.server.processor;


import com.alibaba.fastjson.JSON;
import com.apple.eawt.Application;
import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.common.ServerNodeRole;
import com.pantheon.common.protocol.RequestCode;
import com.pantheon.common.protocol.ResponseCode;
import com.pantheon.common.protocol.header.*;
import com.pantheon.common.protocol.heartBeat.ServiceHeartBeat;
import com.pantheon.remoting.common.RemotingHelper;
import com.pantheon.remoting.exception.RemotingCommandException;
import com.pantheon.remoting.netty.AsyncNettyRequestProcessor;
import com.pantheon.remoting.netty.NettyRequestProcessor;
import com.pantheon.remoting.protocol.RemotingCommand;
import com.pantheon.server.ServerNode;
import com.pantheon.server.node.Controller;
import com.pantheon.server.node.ControllerCandidate;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author Anthony
 * @create 2021/11/19
 * @desc process the connection between client and server
 **/
public class ClientManageProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(ClientManageProcessor.class);

    public ClientManageProcessor() {
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()) {
            case RequestCode.SERVICE_REGISTRY:
                return this.serviceRegistry(ctx, request);
            case RequestCode.HEART_BEAT:
                return this.heartBeat(ctx, request);
            case RequestCode.SERVICE_UNREGISTER:
                return this.serviceUnregister(ctx, request);
            default:
                break;
        }
        return null;
    }

    private RemotingCommand serviceUnregister(ChannelHandlerContext ctx, RemotingCommand request) {
        return null;
    }

    private RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        ServiceHeartBeat heartbeatData = ServiceHeartBeat.decode(request.getBody(), ServiceHeartBeat.class);
        logger.info("receive heartbeat from: {}",heartbeatData.getClientId());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        response.setOpaque(request.getOpaque());
        return response;
    }

    private RemotingCommand serviceRegistry(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        List<String> serverAddresses = null;
        final RemotingCommand response = RemotingCommand.createResponseCommand(ServiceRegistryResponseHeader.class);
        final ServiceRegistryRequestHeader requestHeader =
                (ServiceRegistryRequestHeader) request.decodeCommandCustomHeader(ServiceRegistryRequestHeader.class);
        logger.info("serviceRegistry called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        final ServiceRegistryResponseHeader responseHeader = (ServiceRegistryResponseHeader) response.readCustomHeader();
        //todo add more operation here
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        response.setOpaque(request.getOpaque());
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }


}