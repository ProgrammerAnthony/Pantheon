package com.pantheon.server.processor;


import com.pantheon.client.appinfo.Application;
import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.common.ObjectUtils;
import com.pantheon.common.protocol.RequestCode;
import com.pantheon.common.protocol.ResponseCode;
import com.pantheon.common.protocol.heartBeat.ServiceIdentifier;
import com.pantheon.remoting.common.RemotingHelper;
import com.pantheon.remoting.exception.RemotingCommandException;
import com.pantheon.remoting.netty.AsyncNettyRequestProcessor;
import com.pantheon.remoting.netty.NettyRequestProcessor;
import com.pantheon.remoting.protocol.RemotingCommand;
import com.pantheon.server.registry.*;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;

/**
 * @author Anthony
 * @create 2021/11/19
 * @desc process the connection between client and server
 **/
public class ClientManageProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(ClientManageProcessor.class);
    private final ResponseCache responseCache;
    private final InstanceRegistryImpl instanceRegistry;

    public ClientManageProcessor() {
        this.instanceRegistry = InstanceRegistryImpl.getInstance();
        this.responseCache = instanceRegistry.getResponseCache();
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()) {
            case RequestCode.SERVICE_REGISTRY:
                return this.serviceRegistry(ctx, request);
            case RequestCode.SERVICE_HEART_BEAT:
                return this.heartBeat(ctx, request);
            case RequestCode.GET_ALL_APP:
                return this.getApplications(ctx, request);
            case RequestCode.GET_DELTA_APP:
                return this.getDeltaApplications(ctx, request);
            case RequestCode.SERVICE_UNREGISTER:
                return this.serviceUnregister(ctx, request);
            default:
                break;
        }
        return null;
    }

    private RemotingCommand getApplications(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        logger.info("getApplications request from : {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        response.setBody(getApplications());
        response.setOpaque(request.getOpaque());
        return response;
    }

    private RemotingCommand getDeltaApplications(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        logger.info("getDeltaApplications request from : {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        response.setBody(getApplicationsDelta());
        response.setOpaque(request.getOpaque());
        return response;
    }

    private RemotingCommand serviceUnregister(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        ServiceIdentifier serviceIdentifier = ServiceIdentifier.decode(request.getBody(), ServiceIdentifier.class);
        logger.info("receive serviceUnregister from: {} / {}", serviceIdentifier.getAppName(), serviceIdentifier.getInstanceId());
        cancelLease(serviceIdentifier.getAppName(), serviceIdentifier.getInstanceId());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        response.setOpaque(request.getOpaque());
        return response;
    }

    private RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        ServiceIdentifier serviceIdentifier = ServiceIdentifier.decode(request.getBody(), ServiceIdentifier.class);
        logger.info("receive heartBeat from: {} / {}", serviceIdentifier.getAppName(), serviceIdentifier.getInstanceId());
        boolean renew = renew(serviceIdentifier.getAppName(), serviceIdentifier.getInstanceId());
        if (renew) {
            response.setCode(ResponseCode.SUCCESS);
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
        }
        response.setRemark(null);
        response.setOpaque(request.getOpaque());
        return response;
    }

    private RemotingCommand serviceRegistry(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        InstanceInfo instanceInfo = InstanceInfo.decode(request.getBody(), InstanceInfo.class);
        logger.info("serviceRegistry called by {},receive instanceInfo: {} ", RemotingHelper.parseChannelRemoteAddr(ctx.channel()), instanceInfo.toString());
        if (ObjectUtils.isEmpty(instanceInfo)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(null);
            response.setOpaque(request.getOpaque());
        } else {
            register(instanceInfo.getAppName(), instanceInfo);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            response.setOpaque(request.getOpaque());
        }
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
     *
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
    public Response renewLease(
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
            response = this.validateDirtyTimestamp(appName, id, Long.valueOf(lastDirtyTimestamp));
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

    /**
     * Registers information about a particular instance for an
     * {@link Application}.
     *
     * @param info {@link InstanceInfo} information of the instance.
     */
    public Boolean register(String appName, InstanceInfo info) {
        logger.debug("Registering instance with instanceId {} ", info.getId());
        // validate that the instanceinfo contains all the necessary required fields
        if (ObjectUtils.isEmpty(info.getId())) {
            throw new IllegalArgumentException("Missing instanceId");
        } else if (ObjectUtils.isEmpty(info.getHostName())) {
            throw new IllegalArgumentException("Missing hostname");
        } else if (ObjectUtils.isEmpty(info.getIPAddr())) {
            throw new IllegalArgumentException("Missing ip address");
        } else if (ObjectUtils.isEmpty(info.getAppName())) {
            throw new IllegalArgumentException("Missing appName");
        } else if (!appName.equals(info.getAppName())) {
            throw new IllegalArgumentException("Mismatched appName, expecting " + appName + " but was " + info.getAppName());
        }

        instanceRegistry.register(info);
        return true;
    }


    /**
     * Handles {@link InstanceInfo.InstanceStatus} updates.
     *
     * <p>
     * The status updates are normally done for administrative purposes to
     * change the instance status between {@link InstanceInfo.InstanceStatus#UP} and
     * {@link InstanceInfo.InstanceStatus#OUT_OF_SERVICE} to select or remove instances for
     * receiving traffic.
     * </p>
     *
     * @param newStatus          the new status of the instance.
     * @param lastDirtyTimestamp last timestamp when this instance information was updated.
     * @return response indicating whether the operation was a success or
     * failure.
     */
    public Response statusUpdate(String appName, String id,
                                 String newStatus, String lastDirtyTimestamp) {
        try {
            if (instanceRegistry.getInstanceByAppAndId(appName, id) == null) {
                logger.warn("Instance not found: {}/{}", appName, id);
                return Response.status(Response.Status.NOT_FOUND).build();
            }
            boolean isSuccess = instanceRegistry.statusUpdate(appName, id,
                    InstanceInfo.InstanceStatus.valueOf(newStatus), lastDirtyTimestamp);

            if (isSuccess) {
                logger.info("Status updated: " + appName + " - " + id
                        + " - " + newStatus);
                return Response.ok().build();
            } else {
                logger.warn("Unable to update status: " + appName + " - "
                        + id + " - " + newStatus);
                return Response.serverError().build();
            }
        } catch (Throwable e) {
            logger.error("Error updating instance {} for status {}", id,
                    newStatus);
            return Response.serverError().build();
        }
    }

    /**
     * Removes status override for an instance, set with
     * {@link #statusUpdate(String, String, String, String)}.
     *
     * @param lastDirtyTimestamp last timestamp when this instance information was updated.
     * @return response indicating whether the operation was a success or
     * failure.
     */
    public Response deleteStatusUpdate(String appName, String id,
                                       String newStatusValue,
                                       String lastDirtyTimestamp) {
        try {
            if (instanceRegistry.getInstanceByAppAndId(appName, id) == null) {
                logger.warn("Instance not found: {}/{}", appName, id);
                return Response.status(Response.Status.NOT_FOUND).build();
            }

            InstanceInfo.InstanceStatus newStatus = newStatusValue == null ? InstanceInfo.InstanceStatus.UNKNOWN : InstanceInfo.InstanceStatus.valueOf(newStatusValue);
            boolean isSuccess = instanceRegistry.deleteStatusOverride(appName, id,
                    newStatus, lastDirtyTimestamp);

            if (isSuccess) {
                logger.info("Status override removed: " + appName + " - " + id);
                return Response.ok().build();
            } else {
                logger.warn("Unable to remove status override: " + appName + " - " + id);
                return Response.serverError().build();
            }
        } catch (Throwable e) {
            logger.error("Error removing instance's {} status override", id);
            return Response.serverError().build();
        }
    }


    /**
     * Handles cancellation of leases for this particular instance.
     *
     * @return response indicating whether the operation was a success or
     * failure.
     */
    public Boolean cancelLease(String appName, String id) {
        boolean isSuccess = instanceRegistry.cancel(appName, id);

        if (isSuccess) {
            logger.debug("Found (Cancel): " + appName + " - " + id);
            return true;
        } else {
            logger.info("Not Found (Cancel): " + appName + " - " + id);
            return false;
        }
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