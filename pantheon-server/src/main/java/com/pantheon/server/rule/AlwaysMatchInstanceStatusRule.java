package com.pantheon.server.rule;


import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.server.lease.Lease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This rule matches always and returns the current status of the instance.
 *
 */
public class AlwaysMatchInstanceStatusRule implements InstanceStatusOverrideRule {
    private static final Logger logger = LoggerFactory.getLogger(AlwaysMatchInstanceStatusRule.class);

    @Override
    public StatusOverrideResult apply(InstanceInfo instanceInfo,
                                      Lease<InstanceInfo> existingLease) {
        logger.debug("Returning the default instance status {} for instance {}", instanceInfo.getStatus(),
                instanceInfo.getId());
        return StatusOverrideResult.matchingStatus(instanceInfo.getStatus());
    }

    @Override
    public String toString() {
        return AlwaysMatchInstanceStatusRule.class.getName();
    }
}
