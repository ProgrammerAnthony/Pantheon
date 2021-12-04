package com.pantheon.server.registry;


import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.server.lease.Lease;
import com.pantheon.server.rule.InstanceStatusOverrideRule;
import com.pantheon.server.rule.StatusOverrideResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This rule matches if we have an existing lease for the instance that is UP or OUT_OF_SERVICE.
 */
public class LeaseExistsRule implements InstanceStatusOverrideRule {

    private static final Logger logger = LoggerFactory.getLogger(LeaseExistsRule.class);

    @Override
    public StatusOverrideResult apply(InstanceInfo instanceInfo,
                                      Lease<InstanceInfo> existingLease) {
        InstanceInfo.InstanceStatus existingStatus = null;
        if (existingLease != null) {
            existingStatus = existingLease.getHolder().getStatus();
        }
        // Allow server to have its way when the status is UP or OUT_OF_SERVICE
        if ((existingStatus != null)
                && (InstanceInfo.InstanceStatus.OUT_OF_SERVICE.equals(existingStatus)
                || InstanceInfo.InstanceStatus.UP.equals(existingStatus))) {
            logger.debug("There is already an existing lease with status {}  for instance {}",
                    existingLease.getHolder().getStatus().name(),
                    existingLease.getHolder().getId());
            return StatusOverrideResult.matchingStatus(existingLease.getHolder().getStatus());
        }
        return StatusOverrideResult.NO_MATCH;
    }

    @Override
    public String toString() {
        return LeaseExistsRule.class.getName();
    }
}
