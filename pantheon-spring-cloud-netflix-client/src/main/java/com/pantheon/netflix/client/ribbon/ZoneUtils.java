package com.pantheon.netflix.client.ribbon;

import org.springframework.util.StringUtils;

/**
 * @author Anthony
 * @create 2021/12/20
 * @desc
 **/
public class ZoneUtils {
    public ZoneUtils() {
    }

    public static String extractApproximateZone(String host) {
        String[] split = StringUtils.split(host, ".");
        return split == null ? host : split[1];
    }
}
