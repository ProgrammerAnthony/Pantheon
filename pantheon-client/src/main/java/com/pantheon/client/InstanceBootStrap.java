package com.pantheon.client;

import com.pantheon.client.config.DefaultInstanceConfig;
import com.pantheon.common.ShutdownHookThread;
import com.pantheon.remoting.CommandCustomHeader;
import com.pantheon.remoting.InvokeCallback;
import com.pantheon.remoting.RemotingClient;
import com.pantheon.remoting.annotation.CFNullable;
import com.pantheon.remoting.exception.*;
import com.pantheon.remoting.netty.NettyClientConfig;
import com.pantheon.remoting.netty.ResponseFuture;
import com.pantheon.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

/**
 * @author Anthony
 * @create 2021/11/17
 * @desc todo how to subscribe to server’s data and get notified
 **/
public class InstanceBootStrap {
    private static final Logger logger = LoggerFactory.getLogger(InstanceBootStrap.class);

    static RemotingClient remotingClient;

    public static void main(String[] args) {
        logger.info("InstanceBootStrap initializing......");
        DefaultInstanceConfig instanceConfig = DefaultInstanceConfig.getInstance();
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        InstanceNode instanceNode = new InstanceNode(nettyClientConfig, instanceConfig);
        startClientNode(instanceNode);
    }

    private static InstanceNode startClientNode(InstanceNode instanceNode) {
        boolean initResult = instanceNode.initialize();
        if (!initResult) {
            instanceNode.shutdown();
            System.exit(-3);
        }
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(logger, (Callable<Void>) () -> {
            instanceNode.shutdown();
            return null;
        }));
        //start netty

        return instanceNode;
    }


    public static void testInvokeSync() throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setCount(1);
        requestHeader.setMessageTitle("Welcome");
        RemotingCommand request = RemotingCommand.createRequestCommand(0, requestHeader);
        RemotingCommand response = remotingClient.invokeSync("localhost:9991", request, 1000 * 3);
        System.out.println(response);
    }


    public void testInvokeOneway() throws InterruptedException, RemotingConnectException,
            RemotingTimeoutException, RemotingTooMuchRequestException, RemotingSendRequestException {

        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        request.setRemark("messi");
        remotingClient.invokeOneway("localhost:9991", request, 1000 * 3);
    }


    public void testInvokeAsync() throws InterruptedException, RemotingConnectException,
            RemotingTimeoutException, RemotingTooMuchRequestException, RemotingSendRequestException {

        final CountDownLatch latch = new CountDownLatch(1);
        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        request.setRemark("messi");
        remotingClient.invokeAsync("localhost:9991", request, 1000 * 3, new InvokeCallback() {
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
