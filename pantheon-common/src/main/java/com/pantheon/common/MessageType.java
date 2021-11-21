package com.pantheon.common;


public class MessageType {

    public static final Integer TERMINATE = -1;

    public static final Integer VOTE = 1;

    public static final Integer SLOTS_ALLOCATION = 2;

    public static final Integer NODE_SLOTS = 3;

    public static final Integer SLOTS_REPLICA_ALLOCATION = 4;

    public static final Integer NODE_SLOTS_REPLICAS = 5;

    public static final Integer REPLICA_NODE_ID = 6;

    public static final Integer REPLICA_REGISTER = 7;

    public static final Integer REPLICA_HEARTBEAT = 8;

    public static final Integer REPLICA_NODE_IDS = 9;

    public static final Integer CONTROLLER_NODE_ID = 10;

    public static final Integer CHANGE_REPLICA_TO_SLOTS = 11;

    public static final Integer REFRESH_REPLICA_NODE_ID = 12;

    public static final Integer REFRESH_REPLICA_SLOTS = 13;

    public static final Integer REQUEST_SLOTS_DATA = 14;

    public static final Integer UPDATE_NODE_SLOTS = 15;

    public static final Integer UPDATE_REPLICA_NODE_ID = 16;

    public static final Integer TRANSFER_SLOTS = 17;

    public static final Integer UPDATE_SLOTS = 18;

}
