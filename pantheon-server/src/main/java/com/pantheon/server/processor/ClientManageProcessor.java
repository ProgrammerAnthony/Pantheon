package com.pantheon.server.processor;


import com.pantheon.client.appinfo.Application;
import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.common.ObjectUtils;
import com.pantheon.common.protocol.RequestCode;
import com.pantheon.common.protocol.ResponseCode;
import com.pantheon.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import com.pantheon.common.protocol.heartBeat.HeartBeat;
import com.pantheon.common.protocol.heartBeat.ServiceUnregister;
import com.pantheon.remoting.common.RemotingHelper;
import com.pantheon.remoting.exception.RemotingCommandException;
import com.pantheon.remoting.exception.RemotingTimeoutException;
import com.pantheon.remoting.netty.AsyncNettyRequestProcessor;
import com.pantheon.remoting.netty.NettyRequestProcessor;
import com.pantheon.remoting.protocol.RemotingCommand;
import com.pantheon.server.ServerNode;
import com.pantheon.server.client.ClientChannelInfo;
import com.pantheon.server.registry.*;
import com.pantheon.server.slot.SlotManager;
import io.netty.channel.Channel;
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
    private final RouteInstanceToSlotRegistry routeInstanceToSlotRegistry;
    private final ServerNode serverNode;


    public ClientManageProcessor(ServerNode serverNode) {
        this.routeInstanceToSlotRegistry = RouteInstanceToSlotRegistry.getInstance();
        this.serverNode = serverNode;
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
            //todo case only load some apps
            case RequestCode.SERVICE_UNREGISTER:
                return this.serviceUnregister(ctx, request);

//            case RequestCode.GET_CONSUMER_RUNNING_INFO:
//                return this.getConsumerInfo(ctx, request);
            default:
                break;
        }
        return null;
    }

    private RemotingCommand getConsumerInfo(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final GetConsumerRunningInfoRequestHeader requestHeader =
                (GetConsumerRunningInfoRequestHeader) request.decodeCommandCustomHeader(GetConsumerRunningInfoRequestHeader.class);

        return this.callConsumer(RequestCode.GET_CONSUMER_RUNNING_INFO, request, requestHeader.getClientId());
    }

    private RemotingCommand getApplications(ChannelHandlerContext ctx, RemotingCommand request) {
        String clientId = this.serverNode.getConsumerInfoManager().findClientId(ctx.channel());
        SlotManager slotManager = SlotManager.getInstance();

        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        logger.info("getApplications request from : {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
//        response.setBody(instanceRegistry.getApplicationsData());
        response.setOpaque(request.getOpaque());
        return response;
    }

    private RemotingCommand getDeltaApplications(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        logger.info("getDeltaApplications request from : {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
//        response.setBody(instanceRegistry.getApplicationsDeltaData());
        response.setOpaque(request.getOpaque());
        return response;
    }

    private RemotingCommand serviceUnregister(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        ServiceUnregister serviceUnregister = ServiceUnregister.decode(request.getBody(), ServiceUnregister.class);
        logger.info("receive serviceUnregister from: {} / {}", serviceUnregister.getServiceName(), serviceUnregister.getInstanceId());
        cancelLease(serviceUnregister.getServiceName(), serviceUnregister.getInstanceId());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        response.setOpaque(request.getOpaque());
        return response;
    }

    private RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        HeartBeat heartBeat = HeartBeat.decode(request.getBody(), HeartBeat.class);

        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                ctx.channel(),
                heartBeat.getInstanceId(),
                request.getLanguage(),
                request.getVersion()
        );


        logger.debug("receive heartBeat from: {} / {}", heartBeat.getServiceName(), heartBeat.getInstanceId());
        String renewError = routeInstanceToSlotRegistry.renew(heartBeat.getServiceName(), heartBeat.getInstanceId());
        if (renewError == null) {
            response.setCode(ResponseCode.SUCCESS);
        } else {
            response.setRemark(renewError);
            response.setCode(ResponseCode.SYSTEM_ERROR);
        }
        response.setRemark(null);
        response.setOpaque(request.getOpaque());

        this.serverNode.getConsumerInfoManager().registerConsumer(clientChannelInfo, heartBeat.getSubscriptionDataSet());
        return response;
    }

    private RemotingCommand serviceRegistry(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        InstanceInfo instanceInfo = InstanceInfo.decode(request.getBody(), InstanceInfo.class);
        logger.debug("serviceRegistry called by {},receive instanceInfo: {} ", RemotingHelper.parseChannelRemoteAddr(ctx.channel()), instanceInfo.toString());
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
     * Get requests returns the information about the instance's
     * {@link InstanceInfo}.
     *
     * @return response containing information about the the instance's
     * {@link InstanceInfo}.
     */
    public InstanceInfo getInstanceInfo(final String appName, final String id) {
        InstanceInfo appInfo = routeInstanceToSlotRegistry
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
        String renewError = routeInstanceToSlotRegistry.renew(appName, id);

        // Not found in the registry, immediately ask for a register
        if (renewError != null) {
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
                routeInstanceToSlotRegistry.storeOverriddenStatusIfRequired(appName, id, InstanceInfo.InstanceStatus.valueOf(overriddenStatus));
            }
        } else {
            response = Response.ok().build();
        }
        logger.debug("Found (Renew): {} - {}; reply status={}" + appName, id, response.getStatus());
        return response;
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
        } else if (ObjectUtils.isEmpty(info.getSlotNum())) {
            throw new IllegalArgumentException("Missing slotNum");
        }

        routeInstanceToSlotRegistry.register(info);
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
            if (routeInstanceToSlotRegistry.getInstanceByAppAndId(appName, id) == null) {
                logger.warn("Instance not found: {}/{}", appName, id);
                return Response.status(Response.Status.NOT_FOUND).build();
            }
            boolean isSuccess = routeInstanceToSlotRegistry.statusUpdate(appName, id,
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
            if (routeInstanceToSlotRegistry.getInstanceByAppAndId(appName, id) == null) {
                logger.warn("Instance not found: {}/{}", appName, id);
                return Response.status(Response.Status.NOT_FOUND).build();
            }

            InstanceInfo.InstanceStatus newStatus = newStatusValue == null ? InstanceInfo.InstanceStatus.UNKNOWN : InstanceInfo.InstanceStatus.valueOf(newStatusValue);
            boolean isSuccess = routeInstanceToSlotRegistry.deleteStatusOverride(appName, id,
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
        boolean isSuccess = routeInstanceToSlotRegistry.cancel(appName, id);

        if (isSuccess) {
            logger.debug("Found (Cancel): " + appName + " - " + id);
            return true;
        } else {
            logger.info("Not Found (Cancel): " + appName + " - " + id);
            return false;
        }
    }


    private Response validateDirtyTimestamp(String appName, String id, Long lastDirtyTimestamp) {
        InstanceInfo appInfo = routeInstanceToSlotRegistry.getInstanceByAppAndId(appName, id);
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

    public RemotingCommand callConsumer(
            final int requestCode,
            final RemotingCommand request,
            final String clientId) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        ClientChannelInfo clientChannelInfo = this.serverNode.getConsumerInfoManager().findChannel(clientId);

        if (null == clientChannelInfo) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The Consumer <%s> not online", clientId));
            return response;
        }

        try {
            RemotingCommand newRequest = RemotingCommand.createRequestCommand(requestCode, null);
            newRequest.setExtFields(request.getExtFields());
            newRequest.setBody(request.getBody());

            return this.serverNode.getServerToClient().callClient(clientChannelInfo.getChannel(), newRequest);
        } catch (RemotingTimeoutException e) {
            response.setCode(ResponseCode.CONSUME_MSG_TIMEOUT);
            response
                    .setRemark(String.format("consumer <%s> Timeout: %s", clientId, RemotingHelper.exceptionSimpleDesc(e)));
            return response;
        } catch (Exception e) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(
                    String.format("invoke consumer <%s> Exception: %s", clientId, RemotingHelper.exceptionSimpleDesc(e)));
            return response;
        }
    }

    public void callConsumer(
            final int requestCode,
            final RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        List<Channel> allChannel = this.serverNode.getConsumerInfoManager().getAllChannel();
        for (Channel channel : allChannel) {
            try {
                final GetConsumerRunningInfoRequestHeader requestHeader =
                        (GetConsumerRunningInfoRequestHeader) request.decodeCommandCustomHeader(GetConsumerRunningInfoRequestHeader.class);
                requestHeader.setClientId(this.serverNode.getConsumerInfoManager().findClientId(channel));
                RemotingCommand newRequest = RemotingCommand.createRequestCommand(requestCode, requestHeader);
                newRequest.setExtFields(request.getExtFields());
                newRequest.setBody(request.getBody());

                RemotingCommand remotingCommand = this.serverNode.getServerToClient().callClient(channel, newRequest);
                if (remotingCommand != null) {
//                    logger.info("getConsumerRunningInfo response from client: " + remotingCommand.getRemark());
                }
            } catch (RemotingTimeoutException e) {
                response.setCode(ResponseCode.CONSUME_MSG_TIMEOUT);
                response
                        .setRemark(String.format("consumer <%s> Timeout: %s", channel, RemotingHelper.exceptionSimpleDesc(e)));
            } catch (Exception e) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(
                        String.format("invoke consumer <%s> Exception: %s", channel, RemotingHelper.exceptionSimpleDesc(e)));
            }
        }
    }


}