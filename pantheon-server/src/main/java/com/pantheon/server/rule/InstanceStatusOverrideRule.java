package com.pantheon.server.rule;


import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.server.lease.Lease;
import com.pantheon.server.registry.InstanceRegistryImpl;

/**
 * A single rule that if matched it returns an instance status.
 * The idea is to use an ordered list of such rules and pick the first result that matches.
 *
 * It is designed to be used by
 * {@link InstanceRegistryImpl#getOverriddenInstanceStatus(InstanceInfo, Lease)}
 *
 */
public interface InstanceStatusOverrideRule {

    /**
     * Match this rule.
     *
     * @param instanceInfo The instance info whose status we care about.
     * @param existingLease Does the instance have an existing lease already? If so let's consider that.
     * @return A result with whether we matched and what we propose the status to be overriden to.
     */
    StatusOverrideResult apply(final InstanceInfo instanceInfo,
                               final Lease<InstanceInfo> existingLease);

}
