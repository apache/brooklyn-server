package org.apache.brooklyn.core.workflow;

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.util.core.predicates.DslPredicates;

import java.util.Map;

public interface WorkflowCommonConfig {

    public static final ConfigKey<Map<String,Object>> STEPS = ConfigKeys.newConfigKey(new TypeToken<Map<String,Object>>() {}, "steps",
            "Map of step ID to step body defining this workflow");

    // TODO
//    //    timeout:  a duration, after which the task is interrupted (and should cancel the task); if omitted, there is no explicit timeout at a step (the containing workflow may have a timeout)
//    protected Duration timeout;
//    public Duration getTimeout() {
//        return timeout;
//    }

    public static final ConfigKey<DslPredicates.DslPredicate> CONDITION = ConfigKeys.newConfigKey(DslPredicates.DslPredicate.class, "condition",
            "Condition required on the entity where this effector is placed in order for the effector to be invocable");

    // TODO
//    on-error:  a description of how to handle errors section
//    log-marker:

}
