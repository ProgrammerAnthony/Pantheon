package com.pantheon.netflix.client.loadbalancer;

import com.netflix.loadbalancer.Server;
import com.netflix.loadbalancer.ZoneAffinityServerListFilter;

/**
 * @author Anthony
 * @create 2021/12/20
 * @desc The Default NIWS Filter - deals with filtering out servers based on the Zone affinity and other related properties
 **/
public class DefaultNIWSServerListFilter<T extends Server> extends ZoneAffinityServerListFilter<T> {
    public DefaultNIWSServerListFilter() {
    }
}