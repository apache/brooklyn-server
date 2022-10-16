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
import org.apache.brooklyn.util.time.Duration;

import java.util.List;
import java.util.Map;

public interface WorkflowCommonConfig {

    ConfigKey<Map<String,Object>> PARAMETER_DEFS = AddEffectorInitializerAbstractProto.EFFECTOR_PARAMETER_DEFS;
    ConfigKey<Map<String,Object>> INPUT = new MapConfigKey<Object>(Object.class, "input");
    ConfigKey<Object> OUTPUT = ConfigKeys.newConfigKey(Object.class, "output");

    ConfigKey<List<Object>> STEPS = ConfigKeys.newConfigKey(new TypeToken<List<Object>>() {}, "steps",
            "List of step definitions (string or map) defining a workflow");

    ConfigKey<DslPredicates.DslPredicate> CONDITION = ConfigKeys.newConfigKey(DslPredicates.DslPredicate.class, "condition",
            "Condition required for this workflow to run");

    ConfigKey<WorkflowReplayUtils.ReplayableOption> REPLAYABLE = ConfigKeys.newConfigKey(WorkflowReplayUtils.ReplayableOption.class, "replayable",
            "Indication of from what points the workflow is replayable");

    ConfigKey<List<Object>> ON_ERROR = ConfigKeys.newConfigKey(new TypeToken<List<Object>>() {}, "on-error",
            "List of potential error handlers");

    ConfigKey<Duration> TIMEOUT = ConfigKeys.newConfigKey(Duration.class, "timeout",
            "Time after which a workflow should be automatically interrupted and failed");

}
