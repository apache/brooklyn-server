/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.core.workflow;

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.effector.AddEffectorInitializerAbstractProto;
import org.apache.brooklyn.util.core.predicates.DslPredicates;

import java.util.Map;

public interface WorkflowCommonConfig {

    ConfigKey<Map<String,Object>> PARAMETER_DEFS = AddEffectorInitializerAbstractProto.EFFECTOR_PARAMETER_DEFS;
    ConfigKey<Map<String,Object>> INPUT = new MapConfigKey<Object>(Object.class, "input");
    ConfigKey<Object> OUTPUT = ConfigKeys.newConfigKey(Object.class, "output");

    ConfigKey<Map<String,Object>> STEPS = ConfigKeys.newConfigKey(new TypeToken<Map<String,Object>>() {}, "steps",
            "Map of step ID to step body defining this workflow");

    // TODO
//    //    timeout:  a duration, after which the task is interrupted (and should cancel the task); if omitted, there is no explicit timeout at a step (the containing workflow may have a timeout)
//    protected Duration timeout;
//    public Duration getTimeout() {
//        return timeout;
//    }

    ConfigKey<DslPredicates.DslPredicate> CONDITION = ConfigKeys.newConfigKey(DslPredicates.DslPredicate.class, "condition",
            "Condition required on the entity where this effector is placed in order for the effector to be invocable");

    // TODO
//    on-error:  a description of how to handle errors section
//    log-marker:

}
