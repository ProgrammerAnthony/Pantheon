package com.pantheon.netflix.client.loadbalancer;

import com.netflix.loadbalancer.Server;
import com.netflix.loadbalancer.ZoneAffinityServerListFilter;

/**
 * @author Anthony
 * @create 2021/12/20
 * @desc
 **/
public class DefaultNIWSServerListFilter<T extends Server> extends ZoneAffinityServerListFilter<T> {
    public DefaultNIWSServerListFilter() {
    }
}