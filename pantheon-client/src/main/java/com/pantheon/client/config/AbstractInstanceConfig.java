package com.pantheon.client.config;

import com.pantheon.client.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Anthony
 * @create 2021/11/17
 * @desc An abstract instance info configuration with some defaults to get the users
 * started quickly.The users have to override only a few methods to register
 * their instance with pantheon server.
 **/
public abstract class AbstractInstanceConfig implements PantheonInstanceConfig {
    private static final Logger logger = LoggerFactory.getLogger(AbstractInstanceConfig.class);

    private static final int LEASE_EXPIRATION_DURATION_SECONDS = 90;
    private static final int LEASE_RENEWAL_INTERVAL_SECONDS = 30;
    private static final Pair<String, String> hostInfo = getHostInfo();

    protected AbstractInstanceConfig() {

    }

    @Override
    public String getHostName(boolean refresh) {
        return hostInfo.second();
    }

    @Override
    public String getIpAddress() {
        return hostInfo.first();
    }

    @Override
    public int getLeaseRenewalIntervalInSeconds() {
        return LEASE_RENEWAL_INTERVAL_SECONDS;
    }

    @Override
    public int getLeaseExpirationDurationInSeconds() {
        return LEASE_EXPIRATION_DURATION_SECONDS;
    }

    private static Pair<String, String> getHostInfo() {
        Pair<String, String> pair;
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            pair = new Pair<String, String>(localHost.getHostAddress(), localHost.getHostName());
        } catch (UnknownHostException e) {
            logger.error("Cannot get host info", e);
            pair = new Pair<String, String>("", "");
        }
        return pair;
    }
}
