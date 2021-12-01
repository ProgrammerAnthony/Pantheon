package com.pantheon.server.processor;


import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.common.protocol.RequestCode;
import com.pantheon.common.protocol.ResponseCode;
import com.pantheon.common.protocol.header.*;
import com.pantheon.common.protocol.heartBeat.ServiceHeartBeat;
import com.pantheon.remoting.common.RemotingHelper;
import com.pantheon.remoting.exception.RemotingCommandException;
import com.pantheon.remoting.netty.AsyncNettyRequestProcessor;
import com.pantheon.remoting.netty.NettyRequestProcessor;
import com.pantheon.remoting.protocol.RemotingCommand;
import com.pantheon.server.registry.InstanceRegistry;
import com.pantheon.server.registry.Key;
import com.pantheon.server.registry.ResponseCache;
import com.pantheon.server.registry.ResponseCacheImpl;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.util.List;

/**
 * @author Anthony
 * @create 2021/11/19
 * @desc process the connection between client and server
 **/
public class ClientManageProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(ClientManageProcessor.class);
    private final ResponseCache responseCache;
    private final InstanceRegistry instanceRegistry;

    public ClientManageProcessor() {
        this.instanceRegistry = new InstanceRegistry();
        this.responseCache = instanceRegistry.getResponseCache();
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
        logger.info("receive heartbeat from: {}", heartbeatData.getClientId());
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

    /**
     * get applications info from cache
     *
     * @return
     */
    public byte[] getApplications() {
        Key cacheKey = new Key(
                ResponseCacheImpl.ALL_APPS,
                Key.ACCEPT.COMPACT
        );
        return responseCache.getGZIP(cacheKey);
    }

    /**
     * get applications delta info from cache
     * @return
     */
    public byte[] getApplicationsDelta() {
        Key cacheKey = new Key(
                ResponseCacheImpl.ALL_APPS_DELTA,
                Key.ACCEPT.COMPACT
        );
        return responseCache.getGZIP(cacheKey);
    }


    /**
     * Get requests returns the information about the instance's
     * {@link InstanceInfo}.
     *
     * @return response containing information about the the instance's
     * {@link InstanceInfo}.
     */
    public InstanceInfo getInstanceInfo(final String appName, final String id) {
        InstanceInfo appInfo = instanceRegistry
                .getInstanceByAppAndId(appName, id);
        return appInfo;
    }

    /**
     * A  request for renewing lease from a client instance.
     *
     * @param overriddenStatus   overridden status if any.
     * @param status             the {@link InstanceInfo.InstanceStatus} of the instance.
     * @param lastDirtyTimestamp last timestamp when this instance information was updated.
     * @return response indicating whether the operation was a success or
     * failure.
     */
    public Object renewLease(
            String id,
            String appName,
            String overriddenStatus,
            String status,
            String lastDirtyTimestamp) {
        boolean isSuccess = instanceRegistry.renew(appName, id);

        // Not found in the registry, immediately ask for a register
        if (!isSuccess) {
            logger.warn("Not Found (Renew): {} - {}", appName, id);
            return null;
        }
        // Check if we need to sync based on dirty time stamp, the client
        // instance might have changed some value
        Response response = null;
        if (lastDirtyTimestamp != null) {
            response = this.validateDirtyTimestamp(appName,id,Long.valueOf(lastDirtyTimestamp));
            // Store the overridden status since the validation found out the node that replicates wins
            if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()
                    && (overriddenStatus != null)
                    && !(InstanceInfo.InstanceStatus.UNKNOWN.name().equals(overriddenStatus))) {
                instanceRegistry.storeOverriddenStatusIfRequired(appName, id, InstanceInfo.InstanceStatus.valueOf(overriddenStatus));
            }
        } else {
            response = Response.ok().build();
        }
        logger.debug("Found (Renew): {} - {}; reply status={}" + appName, id, response.getStatus());
        return response;
    }


    public boolean renew(final String appName, final String id) {
        if (instanceRegistry.renew(appName, id)) {
            return true;
        }
        return false;
    }


    private Response validateDirtyTimestamp(String appName, String id, Long lastDirtyTimestamp) {
        InstanceInfo appInfo = instanceRegistry.getInstanceByAppAndId(appName, id);
        if (appInfo != null) {
            if ((lastDirtyTimestamp != null) && (!lastDirtyTimestamp.equals(appInfo.getLastDirtyTimestamp()))) {
                Object[] args = {id, appInfo.getLastDirtyTimestamp(), lastDirtyTimestamp};

                if (lastDirtyTimestamp > appInfo.getLastDirtyTimestamp()) {
                    logger.debug(
                            "Time to sync, since the last dirty timestamp differs -"
                                    + " ReplicationInstance id : {},Registry : {} Incoming: {} ",
                            args);
                    return Response.status(Response.Status.NOT_FOUND).build();
                } else if (appInfo.getLastDirtyTimestamp() > lastDirtyTimestamp) {
                    return Response.ok().build();
                }
            }

        }
        return Response.ok().build();
    }


    @Override
    public boolean rejectRequest() {
        return false;
    }


}