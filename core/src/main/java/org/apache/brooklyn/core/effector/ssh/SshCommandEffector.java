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
package org.apache.brooklyn.core.effector.ssh;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.effector.ParameterType;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.effector.Effectors.EffectorBuilder;
import org.apache.brooklyn.core.effector.ssh.SshEffectorTasks.SshEffectorTaskFactory;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.sensor.ssh.SshCommandSensor;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.json.ShellEnvironmentSerializer;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.yoml.annotations.Alias;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Maps;

@Alias(preferred="ssh-effector")
public final class SshCommandEffector extends AddEffector {

    @Alias({"script", "run"})
    public static final ConfigKey<String> EFFECTOR_COMMAND = ConfigKeys.newStringConfigKey("command");
    public static final ConfigKey<String> EFFECTOR_EXECUTION_DIR = SshCommandSensor.SENSOR_EXECUTION_DIR;
    @Alias(preferred="env", value={"vars","variables","environment"})
    public static final MapConfigKey<String> EFFECTOR_SHELL_ENVIRONMENT = BrooklynConfigKeys.SHELL_ENVIRONMENT_STRING_VALUES;

    public static enum ExecutionTarget {
        ENTITY,
        MEMBERS,
        CHILDREN
    }
    public static final ConfigKey<ExecutionTarget> EXECUTION_TARGET = ConfigKeys.newConfigKey(ExecutionTarget.class, "executionTarget", 
        "Where this command should run; by default on this 'entity'; alternatively on all 'children' or all 'members' (if it's a group); "
        + "in the latter cases the sets are filtered by entities which have a machine and are not stopping.",
        ExecutionTarget.ENTITY);

    public SshCommandEffector(ConfigBag params) {
        super(newEffectorBuilder(params).build());
    }

    public SshCommandEffector(Map<String,String> params) {
        this(ConfigBag.newInstance(params));
    }

    public static EffectorBuilder<String> newEffectorBuilder(ConfigBag params) {
        EffectorBuilder<String> eff = AddEffector.newEffectorBuilder(String.class, params);
        eff.impl(new Body(eff.buildAbstract(), params));
        return eff;
    }

    protected static class Body extends EffectorBody<String> {
        private final Effector<?> effector;
        private final String command;
        private final Map<String, String> shellEnv;
        private final String executionDir;
        private final ExecutionTarget executionTarget;

        public Body(Effector<?> eff, ConfigBag params) {
            this.effector = eff;
            this.command = Preconditions.checkNotNull(params.get(EFFECTOR_COMMAND), "SSH command must be supplied when defining this effector");
            this.shellEnv = params.get(EFFECTOR_SHELL_ENVIRONMENT);
            this.executionDir = params.get(EFFECTOR_EXECUTION_DIR);
            this.executionTarget = params.get(EXECUTION_TARGET);
        }

        @Override
        public String call(ConfigBag params) {
            switch (executionTarget) {
            case ENTITY:
                return callOne(params);
            case MEMBERS:
                return callMany(((Group)entity()).getMembers(), params);
            case CHILDREN:
                return callMany(entity().getChildren(), params);
            default:
                throw new IllegalStateException("Unknown value passed as execution target: " + executionTarget);
            }
        }
        
        public String callOne(ConfigBag params) {
            return queue(
                makePartialTaskFactory(params)
                    .summary("effector "+effector.getName()+" ssh call")
                ).get();
        }
        public String callMany(Collection<Entity> targets, ConfigBag params) {
            TaskBuilder<Object> ptb = Tasks.builder().parallel(true).displayName("effector "+effector.getName()+" ssh to targets");
            for (Entity target: targets) {
                if (Entities.isNoLongerManaged(target)) continue;
                
                Lifecycle state = target.getAttribute(Attributes.SERVICE_STATE_ACTUAL);
                if (state==Lifecycle.STOPPING || state==Lifecycle.STOPPED) continue;

                Maybe<SshMachineLocation> machine = Locations.findUniqueSshMachineLocation(target.getLocations());
                if (machine.isAbsent()) continue;
                
                SshEffectorTaskFactory<String> t = makePartialTaskFactory(params);
                t.summary("effector "+effector.getName()+" at "+target); 
                t.machine( machine.get() );
                
                ptb.add(t.newTask());
            }
            queue(ptb.build()).getUnchecked();
            return null;
        }
        
        public SshEffectorTaskFactory<String> makePartialTaskFactory(ConfigBag params) {
            String sshCommand = SshCommandSensor.makeCommandExecutingInDirectory(command, executionDir, entity());

            MutableMap<String, Object> env = MutableMap.of();

            // Set all declared parameters, including default values
            for (ParameterType<?> param : effector.getParameters()) {
                env.addIfNotNull(param.getName(), params.get(Effectors.asConfigKey(param)));
            }

            // Set things from the entity's defined shell environment, if applicable
            env.putAll(entity().config().get(BrooklynConfigKeys.SHELL_ENVIRONMENT));

            // Set the parameters we've been passed. This will repeat declared parameters but to no harm,
            // it may pick up additional values (could be a flag defining whether this is permitted or not.)
            // Make sure we do not include the shell.env here again, by filtering it out.
            env.putAll(Maps.filterKeys(params.getAllConfig(), Predicates.not(Predicates.equalTo(EFFECTOR_SHELL_ENVIRONMENT.getName()))));

            // Add the shell environment entries from the effector configuration
            if (shellEnv != null) env.putAll(shellEnv);
            
            // Add the shell environment entries from our invocation
            Map<String, String> effectorEnv = params.get(EFFECTOR_SHELL_ENVIRONMENT);
            if (effectorEnv != null) env.putAll(effectorEnv);
            
            // Try to resolve the configuration in the env Map
            try {
                env = MutableMap.copyOf(resolveEnv(env));
            } catch (InterruptedException | ExecutionException e) {
                Exceptions.propagateIfFatal(e);
            }

            // Execute the effector with the serialized environment strings
            ShellEnvironmentSerializer serializer = new ShellEnvironmentSerializer(entity().getManagementContext());
            
            return SshEffectorTasks.ssh(sshCommand)
                    .requiringZeroAndReturningStdout()
                    .environmentVariables(serializer.serialize(env));
        }

        @SuppressWarnings("unchecked")
        private Map<String, Object> resolveEnv(MutableMap<String, Object> env) throws ExecutionException, InterruptedException {
            return (Map<String, Object>) Tasks.resolveDeepValue(env, Object.class, entity().getExecutionContext());
        }
    }
}
