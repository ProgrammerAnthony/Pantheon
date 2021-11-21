package com.pantheon.common.entity;

import java.nio.ByteBuffer;

/**
 * 请求接口
 */
public abstract class Request implements Message {

    /**
     * 请求标识
     */
    public static final Integer REQUEST_FLAG = 1;

    /**
     * 拉取slots分配数据请求
     */
    public static final Integer FETCH_SLOTS_ALLOCATION = 1;
    /**
     * 拉取server节点地址列表请求
     */
    public static final Integer FETCH_SERVER_ADDRESSES = 2;
    /**
     * 服务注册请求
     */
    public static final Integer REGISTER = 3;
    /**
     * 心跳请求
     */
    public static final Integer HEARTBEAT = 4;
    /**
     * 拉取server节点id
     */
    public static final Integer FETCH_SERVER_NODE_ID = 5;
    /**
     * 订阅服务
     */
    public static final Integer SUBSCRIBE = 6;
    /**
     * 服务实例变动
     */
    public static final Integer SERVICE_CHANGED = 7;

    /**
     * 请求标识字节数
     */
    public static final Integer REQUEST_FLAG_BYTES = 4;
    /**
     * 请求长度的字节数
     */
    public static final Integer REQUEST_LENGTH_BYTES = 4;
    /**
     * 请求类型的字节数
     */
    public static final Integer REQUEST_TYPE_BYTES = 4;
    /**
     * 请求ID的字节数
     */
    public static final Integer REQUEST_ID_BYTES = 32;
    /**
     * 字符串类型的请求字段的长度的字节数
     */
    public static final Integer REQUEST_STRING_FIELD_LENGTH_BYTES = 4;
    /**
     * 整数类型的请求字段的字节数
     */
    public static final Integer REQUEST_INTEGER_FIELD_BYTES = 4;

    /**
     * 获取请求id
     * @return
     */
    public abstract String getId();

    public static Request deserialize(Integer requestType, ByteBuffer messageBuffer) {
        Request request = null;

//        if(requestType.equals(Request.FETCH_SLOTS_ALLOCATION)) {
//            request = FetchSlotsAllocationRequest.deserialize(messageBuffer);
//        } else if(requestType.equals(Request.FETCH_SERVER_ADDRESSES)) {
//            request = FetchServerAddressesRequest.deserialize(messageBuffer);
//        } else if(requestType.equals(Request.REGISTER)) {
//            request = RegisterRequest.deserialize(messageBuffer);
//        } else if(requestType.equals(Request.HEARTBEAT)) {
//            request = HeartbeatRequest.deserialize(messageBuffer);
//        } else if(requestType.equals(Request.FETCH_SERVER_NODE_ID)) {
//            request = FetchServerNodeIdRequest.deserialize(messageBuffer);
//        } else if(requestType.equals(Request.SUBSCRIBE)) {
//            request = SubscribeRequest.deserialize(messageBuffer);
//        } else if(requestType.equals(Request.SERVICE_CHANGED)) {
//            request = ServiceChangedRequest.deserialize(messageBuffer);
//        }

        return request;
    }

}
