package com.pantheon.server.rule;


import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.server.lease.Lease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This rule matches if the instance is DOWN or STARTING.
 *
 */
public class DownOrStartingRule implements InstanceStatusOverrideRule {
    private static final Logger logger = LoggerFactory.getLogger(DownOrStartingRule.class);

    @Override
    public StatusOverrideResult apply(InstanceInfo instanceInfo,
                                      Lease<InstanceInfo> existingLease) {
        if ((!InstanceInfo.InstanceStatus.UP.equals(instanceInfo.getStatus()))
                && (!InstanceInfo.InstanceStatus.OUT_OF_SERVICE.equals(instanceInfo.getStatus()))) {
            logger.debug("Trusting the instance status {}  for instance {}",
                    instanceInfo.getStatus(), instanceInfo.getId());
            return StatusOverrideResult.matchingStatus(instanceInfo.getStatus());
        }
        return StatusOverrideResult.NO_MATCH;
    }

    @Override
    public String toString() {
        return DownOrStartingRule.class.getName();
    }
}
