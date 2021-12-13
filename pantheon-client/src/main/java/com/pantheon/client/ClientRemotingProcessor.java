package com.pantheon.client;

import com.pantheon.common.protocol.RequestCode;
import com.pantheon.common.protocol.ResponseCode;
import com.pantheon.remoting.common.RemotingHelper;
import com.pantheon.remoting.netty.AsyncNettyRequestProcessor;
import com.pantheon.remoting.netty.NettyRequestProcessor;
import com.pantheon.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * @author Anthony
 * @create 2021/11/27
 * @desc
 */
public class ClientRemotingProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(ClientRemotingProcessor.class);

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()) {
            case RequestCode.GET_CONSUMER_RUNNING_INFO:
                return this.getConsumerRunningInfo(ctx, request);
            default:
                break;
        }
        return null;
    }

    /**
     * server can load consumer state
     * @param ctx
     * @param request
     * @return
     */
    private RemotingCommand getConsumerRunningInfo(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        logger.info("getConsumerRunningInfo request from : {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        response.setBody("test cliet side response！！！！".getBytes(StandardCharsets.UTF_8));
        response.setOpaque(request.getOpaque());
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
