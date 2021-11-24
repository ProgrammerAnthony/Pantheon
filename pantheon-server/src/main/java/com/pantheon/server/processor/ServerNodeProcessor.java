package com.pantheon.server.processor;


import com.pantheon.common.ServerNodeRole;
import com.pantheon.common.protocol.RequestCode;
import com.pantheon.common.protocol.ResponseCode;
import com.pantheon.common.protocol.header.GetServerNodeIdRequestHeader;
import com.pantheon.common.protocol.header.GetServerNodeIdResponseHeader;
import com.pantheon.common.protocol.header.GetSlotsRequestHeader;
import com.pantheon.common.protocol.header.GetSlotsResponseHeader;
import com.pantheon.remoting.common.RemotingHelper;
import com.pantheon.remoting.exception.RemotingCommandException;
import com.pantheon.remoting.netty.AsyncNettyRequestProcessor;
import com.pantheon.remoting.netty.NettyRequestProcessor;
import com.pantheon.remoting.protocol.RemotingCommand;
import com.pantheon.server.ServerController;
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
public class ServerNodeProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(ServerNodeProcessor.class);
    private final ServerController serverController;

    public ServerNodeProcessor(final ServerController serverController) {
        this.serverController = serverController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()) {
            case RequestCode.GET_SERVER_NODE_ID:
                return this.getServerNodeId(ctx, request);
            case RequestCode.GET_SLOTS_ALLOCATION:
                return this.getSlotsAllocation(ctx, request);
            case RequestCode.GET_SERVER_ADDRESSES:
                return this.getServerAddresses(ctx, request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }


    private synchronized RemotingCommand getServerNodeId(ChannelHandlerContext ctx,
                                                         RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetServerNodeIdResponseHeader.class);
        final GetServerNodeIdRequestHeader requestHeader =
                (GetServerNodeIdRequestHeader) request.decodeCommandCustomHeader(GetServerNodeIdRequestHeader.class);
        logger.info("getServerNodeId called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        final GetServerNodeIdResponseHeader responseHeader = (GetServerNodeIdResponseHeader) response.readCustomHeader();
        responseHeader.setServerNodeId(serverController.getServerConfig().getNodeId());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        response.setOpaque(request.getOpaque());
        return response;
    }

    private synchronized RemotingCommand getSlotsAllocation(ChannelHandlerContext ctx,
                                                            RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetSlotsResponseHeader.class);
        final GetSlotsRequestHeader requestHeader =
                (GetSlotsRequestHeader) request.decodeCommandCustomHeader(GetSlotsRequestHeader.class);
        logger.info("getSlotsAllocation called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        final GetSlotsResponseHeader responseHeader = (GetSlotsResponseHeader) response.readCustomHeader();
        if(ServerController.getServerNodeRole()==ServerNodeRole.CONTROLLER_CANDIDATE_NODE){
            responseHeader.setSlotsAllocation(ControllerCandidate.getInstance().getSlotsAllocation());
        }else if(ServerController.getServerNodeRole()==ServerNodeRole.CONTROLLER_NODE){
            responseHeader.setSlotsAllocation(Controller.getInstance().getSlotsAllocation());
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        response.setOpaque(request.getOpaque());
        return response;
    }

    private synchronized RemotingCommand getServerAddresses(ChannelHandlerContext ctx,
                                                            RemotingCommand request) {
        return null;
    }

}