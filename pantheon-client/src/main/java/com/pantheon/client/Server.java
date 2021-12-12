package com.pantheon.client;

/**
 * @author Anthony
 * @create 2021/11/18
 * @desc
 **/
public class Server {

    /**
     * node id
     */
    private String id;
    /**
     * ip/hostname address
     */
    private String address;
    /**
     * server port
     */
    private int port;

    /**
     * server contains many slots
     */
    private int slotNum;

    public Server(String id, String address, int port) {
        this.id = id;
        this.address = address;
        this.port = port;
    }

    public Server(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getRemoteSocketAddress() {
        return getAddress() + ":" + getPort();
    }

    public int getSlotNum() {
        return slotNum;
    }

    public void setSlotNum(int slotNum) {
        this.slotNum = slotNum;
    }

    @Override
    public String toString() {
        return "Server{" +
                "id=" + id +
                ", address='" + address + '\'' +
                ", port=" + port +
                '}';
    }

}
