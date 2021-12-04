package com.pantheon.server.rule;


import com.pantheon.client.appinfo.InstanceInfo;
import com.pantheon.server.lease.Lease;

import java.util.ArrayList;
import java.util.List;

/**
 * This rule takes an ordered list of rules and returns the result of the first match or the
 * result of the {@link AlwaysMatchInstanceStatusRule}.
 *
 */
public class FirstMatchWinsCompositeRule implements InstanceStatusOverrideRule {

    private final InstanceStatusOverrideRule[] rules;
    private final InstanceStatusOverrideRule defaultRule;
    private final String compositeRuleName;

    public FirstMatchWinsCompositeRule(InstanceStatusOverrideRule... rules) {
        this.rules = rules;
        this.defaultRule = new AlwaysMatchInstanceStatusRule();
        // Let's build up and "cache" the rule name to be used by toString();
        List<String> ruleNames = new ArrayList<>(rules.length+1);
        for (int i = 0; i < rules.length; ++i) {
            ruleNames.add(rules[i].toString());
        }
        ruleNames.add(defaultRule.toString());
        compositeRuleName = ruleNames.toString();
    }

    @Override
    public StatusOverrideResult apply(InstanceInfo instanceInfo,
                                      Lease<InstanceInfo> existingLease) {
        for (int i = 0; i < this.rules.length; ++i) {
            StatusOverrideResult result = this.rules[i].apply(instanceInfo, existingLease);
            if (result.matches()) {
                return result;
            }
        }
        return defaultRule.apply(instanceInfo, existingLease);
    }

    @Override
    public String toString() {
        return this.compositeRuleName;
    }
}
