package com.pantheon.server.processor;


import com.alibaba.fastjson.JSON;
import com.pantheon.common.ServerNodeRole;
import com.pantheon.common.protocol.RequestCode;
import com.pantheon.common.protocol.ResponseCode;
import com.pantheon.common.protocol.header.*;
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

/**
 * @author Anthony
 * @create 2021/11/19
 * @desc process the connection between client and server
 **/
public class ServerNodeProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(ServerNodeProcessor.class);
    private final ServerNode serverNode;

    public ServerNodeProcessor(final ServerNode serverNode) {
        this.serverNode = serverNode;
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
            case RequestCode.SERVICE_REGISTRY:
                return this.serviceRegistry(ctx,request);
            default:
                break;
        }
        return null;
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


    private synchronized RemotingCommand getServerNodeId(ChannelHandlerContext ctx,
                                                         RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetServerNodeIdResponseHeader.class);
        final GetServerNodeIdRequestHeader requestHeader =
                (GetServerNodeIdRequestHeader) request.decodeCommandCustomHeader(GetServerNodeIdRequestHeader.class);
        logger.info("getServerNodeId called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        final GetServerNodeIdResponseHeader responseHeader = (GetServerNodeIdResponseHeader) response.readCustomHeader();
        responseHeader.setServerNodeId(serverNode.getServerConfig().getNodeId());
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
        if(ServerNode.getServerNodeRole()==ServerNodeRole.CONTROLLER_CANDIDATE_NODE){
            responseHeader.setSlotsAllocation(JSON.toJSONString(ControllerCandidate.getInstance().getSlotsAllocation()));
        }else if(ServerNode.getServerNodeRole()==ServerNodeRole.CONTROLLER_NODE){
            responseHeader.setSlotsAllocation(JSON.toJSONString(Controller.getInstance().getSlotsAllocation()));
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        response.setOpaque(request.getOpaque());
        return response;
    }

    private synchronized RemotingCommand getServerAddresses(ChannelHandlerContext ctx,
                                                            RemotingCommand request) throws RemotingCommandException {
        List<String> serverAddresses = null;
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetServerAddressResponseHeader.class);
        final GetServerAddressRequestHeader requestHeader =
                (GetServerAddressRequestHeader) request.decodeCommandCustomHeader(GetServerAddressRequestHeader.class);
        logger.info("getServerAddresses called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        final GetServerAddressResponseHeader responseHeader = (GetServerAddressResponseHeader) response.readCustomHeader();
        if(ServerNode.getServerNodeRole()==ServerNodeRole.CONTROLLER_CANDIDATE_NODE){
            ControllerCandidate controllerCandidate = ControllerCandidate.getInstance();
            serverAddresses = controllerCandidate.getServerAddresses();
            responseHeader.setServerAddresses(JSON.toJSONString(serverAddresses));
        }else if(ServerNode.getServerNodeRole()==ServerNodeRole.CONTROLLER_NODE){
            Controller controller = Controller.getInstance();
            serverAddresses = controller.getServerAddresses();
            responseHeader.setServerAddresses(JSON.toJSONString(serverAddresses));
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        response.setOpaque(request.getOpaque());
        return response;
    }

}