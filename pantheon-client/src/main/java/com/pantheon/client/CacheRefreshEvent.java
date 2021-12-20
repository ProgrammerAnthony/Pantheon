package com.pantheon.client;

/**
 * @author Anthony
 * @create 2021/12/20
 * @desc
 **/
public class CacheRefreshedEvent implements PantheonEvent {
    public CacheRefreshedEvent() {
    }

    public String toString() {
        return "CacheRefreshedEvent[timestamp=" + System.currentTimeMillis()+ "]";
    }
}
