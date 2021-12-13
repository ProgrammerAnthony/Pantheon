package com.pantheon.server.client;

/**
 * @author Anthony
 * @create 2021/12/13
 * @desc
 */
public class SubscriptionData {
    private Integer slotNum;
    private long subVersion = System.currentTimeMillis();


    public Integer getSlotNum() {
        return slotNum;
    }

    public void setSlotNum(Integer slotNum) {
        this.slotNum = slotNum;
    }

    public long getSubVersion() {
        return subVersion;
    }

    public void setSubVersion(long subVersion) {
        this.subVersion = subVersion;
    }
}
