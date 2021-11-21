package com.pantheon.client;

import com.pantheon.client.config.DefaultInstanceConfig;
import com.pantheon.common.protocol.RequestCode;
import com.pantheon.common.ThreadFactoryImpl;
import com.pantheon.common.protocol.ResponseCode;
import com.pantheon.common.protocol.header.GetServerNodeIdRequestHeader;
import com.pantheon.common.protocol.header.GetServerNodeIdResponseHeader;
import com.pantheon.remoting.CommandCustomHeader;
import com.pantheon.remoting.InvokeCallback;
import com.pantheon.remoting.annotation.CFNullable;
import com.pantheon.remoting.exception.RemotingCommandException;
import com.pantheon.remoting.exception.RemotingConnectException;
import com.pantheon.remoting.exception.RemotingSendRequestException;
import com.pantheon.remoting.exception.RemotingTimeoutException;
import com.pantheon.remoting.exception.RemotingTooMuchRequestException;
import com.pantheon.remoting.netty.NettyClientConfig;
import com.pantheon.remoting.netty.NettyRemotingClient;
import com.pantheon.remoting.netty.ResponseFuture;
import com.pantheon.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.ServerBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Anthony
 * @create 2021/11/19
 * @desc
 **/
public class InstanceController {
    private NettyClientConfig nettyClientConfig;
    private DefaultInstanceConfig instanceConfig;
    private ExecutorService remotingExecutor;
    private NettyRemotingClient remotingClient;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "InstanceControllerScheduledThread"));
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);
    private String excludedRemoteAddress;
    private Server server;

    public InstanceController(NettyClientConfig nettyClientConfig, DefaultInstanceConfig instanceConfig) {
        this.nettyClientConfig = nettyClientConfig;
        this.instanceConfig = instanceConfig;
    }

    public boolean initialize() {
        remotingClient = new NettyRemotingClient(nettyClientConfig);
        this.remotingExecutor =
                Executors.newFixedThreadPool(nettyClientConfig.getClientWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));
        String controllerCandidate = chooseControllerCandidate();
        this.remotingClient.start();
//        server = new Server(controllerCandidate.split(":")[0], Integer.valueOf(controllerCandidate.split(":")[1]));
        Integer nodeId = null;
        try {
            nodeId = fetchServerNodeId(controllerCandidate, 30000);
            logger.info("fetchServerNodeId ");
//            server.setId(nodeId);
            fetchSlotsAllocation(server);
            fetchServerAddresses(server);
            String serviceName = instanceConfig.getServiceName();
            this.server = routeServer(serviceName);
//        if(nodeId.equals(serviceName.))
        } catch (RemotingConnectException e) {
            e.printStackTrace();
        } catch (RemotingSendRequestException e) {
            e.printStackTrace();
        } catch (RemotingTimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (RemotingCommandException e) {
            e.printStackTrace();
        }
        return true;
    }

    private Server routeServer(String serviceName) {
        return null;
    }

    /**
     * fetch server node id
     *
     * @param controllerCandidate server address
     * @return
     */
    private Integer fetchServerNodeId(String controllerCandidate, final long timoutMills) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException {
        GetServerNodeIdRequestHeader requestHeader =new GetServerNodeIdRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SERVER_NODE_ID, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(controllerCandidate, request, timoutMills);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetServerNodeIdResponseHeader responseHeader =
                        (GetServerNodeIdResponseHeader) response.decodeCommandCustomHeader(GetServerNodeIdResponseHeader.class);
                return responseHeader.getServerNodeId();
            }
            default:
                break;
        }
        return null;
    }

    /**
     * fetch slots allocation
     *
     * @param controllerCandidate
     */
    private void fetchSlotsAllocation(Server controllerCandidate) {
        return;
    }

    /**
     * fetch server addresses
     *
     * @param controllerCandidate
     */
    private void fetchServerAddresses(Server controllerCandidate) {
        return;
    }


    public void shutdown() {
        this.remotingClient.shutdown();
        this.remotingExecutor.shutdown();

    }

    public String chooseControllerCandidate() {
        List<String> serverList = instanceConfig.getServerList();
        Random random = new Random();
        boolean chosen = false;
        while (!chosen) {
            int index = random.nextInt(serverList.size());
            String serverAddress = serverList.get(index);

            if (excludedRemoteAddress == null) {
                return serverAddress;
            } else {
                if (serverAddress.equals(excludedRemoteAddress)) {
                    continue;
                } else {
                    return serverAddress;
                }
            }
        }

        return null;
    }

    public void invokeSync() throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        logger.info("sync message to " + instanceConfig.getServerList().get(0));
        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setCount(1);
        requestHeader.setMessageTitle("Welcome");
        RemotingCommand request = RemotingCommand.createRequestCommand(0, requestHeader);
        RemotingCommand response = remotingClient.invokeSync(instanceConfig.getServerList().get(0), request, 1000 * 3);
        System.out.println(response);
    }


    public void invokeOneway() throws InterruptedException, RemotingConnectException,
            RemotingTimeoutException, RemotingTooMuchRequestException, RemotingSendRequestException {

        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        request.setRemark("messi");
        remotingClient.invokeOneway(instanceConfig.getServerList().get(0), request, 1000 * 3);
    }


    public void invokeAsync() throws InterruptedException, RemotingConnectException,
            RemotingTimeoutException, RemotingTooMuchRequestException, RemotingSendRequestException {

        final CountDownLatch latch = new CountDownLatch(1);
        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        request.setRemark("messi");
        remotingClient.invokeAsync(instanceConfig.getServerList().get(0), request, 1000 * 3, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {
                latch.countDown();
                System.out.println(responseFuture.getResponseCommand());
            }
        });
        latch.await();
    }


    static class RequestHeader implements CommandCustomHeader {
        @CFNullable
        private Integer count;

        @CFNullable
        private String messageTitle;

        @Override
        public void checkFields() throws RemotingCommandException {
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        public String getMessageTitle() {
            return messageTitle;
        }

        public void setMessageTitle(String messageTitle) {
            this.messageTitle = messageTitle;
        }
    }
}
