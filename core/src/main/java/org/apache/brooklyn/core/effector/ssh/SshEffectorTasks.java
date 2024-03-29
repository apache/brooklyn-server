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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.objs.Configurable;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.StringConfigMap;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.ConfigPredicates;
import org.apache.brooklyn.core.config.ConfigUtils;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.EffectorTasks;
import org.apache.brooklyn.core.effector.EffectorTasks.EffectorTaskFactory;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.location.internal.LocationInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ssh.SshFetchTaskFactory;
import org.apache.brooklyn.util.core.task.ssh.SshFetchTaskWrapper;
import org.apache.brooklyn.util.core.task.ssh.SshPutTaskFactory;
import org.apache.brooklyn.util.core.task.ssh.SshPutTaskWrapper;
import org.apache.brooklyn.util.core.task.ssh.SshTasks;
import org.apache.brooklyn.util.core.task.ssh.internal.AbstractSshExecTaskFactory;
import org.apache.brooklyn.util.core.task.ssh.internal.PlainSshExecTaskFactory;
import org.apache.brooklyn.util.core.task.system.ProcessTaskFactory;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.ssh.BashCommandsConfigurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.collect.Maps;

/**
 * Conveniences for generating {@link Task} instances to perform SSH activities.
 * <p>
 * If the {@link SshMachineLocation machine} is not specified directly it
 * will be inferred from the {@link Entity} context of either the {@link Effector}
 * or the current {@link Task}.
 * 
 * @see SshTasks
 * @since 0.6.0
 */
@Beta
public class SshEffectorTasks {

    private static final Logger log = LoggerFactory.getLogger(SshEffectorTasks.class);
    
    public static final ConfigKey<Boolean> IGNORE_ENTITY_SSH_FLAGS = ConfigKeys.newBooleanConfigKey("ignoreEntitySshFlags",
        "Whether to ignore any ssh flags (behaviour constraints) set on the entity or location " +
        "where this is running, using only flags explicitly specified", false);
    
    /**
     * Like {@link EffectorBody} but providing conveniences when in an entity with a single machine location.
     */
    public abstract static class SshEffectorBody<T> extends EffectorBody<T> {
        
        /** convenience for accessing the machine */
        public SshMachineLocation machine() {
            return EffectorTasks.getSshMachine(entity());
        }

        /** convenience for generating an {@link PlainSshExecTaskFactory} which can be further customised if desired, and then (it must be explicitly) queued */
        public ProcessTaskFactory<Integer> ssh(String ...commands) {
            return new SshEffectorTaskFactory<Integer>(commands).machine(machine());
        }
    }

    /** variant of {@link PlainSshExecTaskFactory} which fulfills the {@link EffectorTaskFactory} signature so can be used directly as an impl for an effector,
     * also injects the machine automatically; can also be used outwith effector contexts, and machine is still injected if it is
     * run from inside a task at an entity with a single SshMachineLocation */
    public static class SshEffectorTaskFactory<RET> extends AbstractSshExecTaskFactory<SshEffectorTaskFactory<RET>,RET> implements EffectorTaskFactory<RET> {

        public SshEffectorTaskFactory(String ...commands) {
            super(commands);
        }
        public SshEffectorTaskFactory(MachineLocation machine, String ...commands) {
            super(machine, commands);
        }

        @Override
        protected String taskTypeShortName() { return "SSH"; }

        @Override
        public ProcessTaskWrapper<RET> newTask(Entity entity, Effector<RET> effector, ConfigBag parameters) {
            markDirty();
            if (summary==null) summary(effector.getName()+" (ssh)");
            machine(EffectorTasks.getSshMachine(entity));
            return newTask();
        }
        @Override
        public synchronized ProcessTaskWrapper<RET> newTask() {
            dirty = false;
            Entity entity = BrooklynTaskTags.getTargetOrContextEntity(Tasks.current());
            if (machine==null) {
                if (log.isDebugEnabled())
                    log.debug("Using an ssh task not in an effector without any machine; will attempt to infer the machine: "+this);
                if (entity!=null)
                    machine(EffectorTasks.getSshMachine(entity));
            }
            applySshFlags(getConfig(), entity, getRemoteExecCapability().getExtraConfiguration());
            return super.newTask();
        }
        
        @Override
        public <T2> SshEffectorTaskFactory<T2> returning(ScriptReturnType type) {
            return (SshEffectorTaskFactory<T2>) super.<T2>returning(type);
        }

        @Override
        public SshEffectorTaskFactory<Boolean> returningIsExitCodeZero() {
            return (SshEffectorTaskFactory<Boolean>) super.returningIsExitCodeZero();
        }

        @Override
        public SshEffectorTaskFactory<String> requiringZeroAndReturningStdout() {
            return (SshEffectorTaskFactory<String>) super.requiringZeroAndReturningStdout();
        }
        
        @Override
        public <RET2> SshEffectorTaskFactory<RET2> returning(Function<ProcessTaskWrapper<?>, RET2> resultTransformation) {
            return (SshEffectorTaskFactory<RET2>) super.returning(resultTransformation);
        }
    }
    
    public static class SshPutEffectorTaskFactory extends SshPutTaskFactory implements EffectorTaskFactory<Void> {
        public SshPutEffectorTaskFactory(String remoteFile) {
            super(remoteFile);
        }
        public SshPutEffectorTaskFactory(SshMachineLocation machine, String remoteFile) {
            super(machine, remoteFile);
        }
        @Override
        public SshPutTaskWrapper newTask(Entity entity, Effector<Void> effector, ConfigBag parameters) {
            machine(EffectorTasks.getSshMachine(entity));
            applySshFlags(getConfig(), entity, getRemoteExecCapability().getExtraConfiguration());
            return super.newTask();
        }
        @Override
        public SshPutTaskWrapper newTask() {
            Entity entity = BrooklynTaskTags.getTargetOrContextEntity(Tasks.current());
            if (machine==null) {
                if (log.isDebugEnabled())
                    log.debug("Using an ssh put task not in an effector without any machine; will attempt to infer the machine: "+this);
                if (entity!=null) {
                    machine(EffectorTasks.getSshMachine(entity));
                }

            }
            applySshFlags(getConfig(), entity, getRemoteExecCapability().getExtraConfiguration());
            return super.newTask();
        }
    }

    public static class SshFetchEffectorTaskFactory extends SshFetchTaskFactory implements EffectorTaskFactory<String> {
        public SshFetchEffectorTaskFactory(String remoteFile) {
            super(remoteFile);
        }
        public SshFetchEffectorTaskFactory(SshMachineLocation machine, String remoteFile) {
            super(machine, remoteFile);
        }
        @Override
        public SshFetchTaskWrapper newTask(Entity entity, Effector<String> effector, ConfigBag parameters) {
            machine(EffectorTasks.getSshMachine(entity));
            applySshFlags(getConfig(), entity, getRemoteExecCapability().getExtraConfiguration());
            return super.newTask();
        }
        @Override
        public SshFetchTaskWrapper newTask() {
            Entity entity = BrooklynTaskTags.getTargetOrContextEntity(Tasks.current());
            if (machine==null) {
                if (log.isDebugEnabled())
                    log.debug("Using an ssh fetch task not in an effector without any machine; will attempt to infer the machine: "+this);
                if (entity!=null)
                    machine(EffectorTasks.getSshMachine(entity));
            }
            applySshFlags(getConfig(), entity, getRemoteExecCapability().getExtraConfiguration());
            return super.newTask();
        }
    }

    /**
     * @since 0.9.0
     */
    public static SshEffectorTaskFactory<Integer> ssh(SshMachineLocation machine, String ...commands) {
        return new SshEffectorTaskFactory<Integer>(machine, commands);
    }

    public static SshEffectorTaskFactory<Integer> ssh(String ...commands) {
        return new SshEffectorTaskFactory<Integer>(commands);
    }

    public static SshEffectorTaskFactory<Integer> ssh(List<String> commands) {
        return ssh(commands.toArray(new String[commands.size()]));
    }

    public static SshPutTaskFactory put(String remoteFile) {
        return new SshPutEffectorTaskFactory(remoteFile);
    }

    public static SshFetchEffectorTaskFactory fetch(String remoteFile) {
        return new SshFetchEffectorTaskFactory(remoteFile);
    }

    /** task which returns 0 if pid is running */
    public static SshEffectorTaskFactory<Integer> codePidRunning(Integer pid) {
        return ssh("ps -p "+pid).summary("PID "+pid+" is-running check (exit code)").allowingNonZeroExitCode();
    }
    
    /** task which fails if the given PID is not running */
    public static SshEffectorTaskFactory<?> requirePidRunning(Integer pid) {
        return codePidRunning(pid).summary("PID "+pid+" is-running check (required)").requiringExitCodeZero("Process with PID "+pid+" is required to be running");
    }

    /** as {@link #codePidRunning(Integer)} but returning boolean */
    public static SshEffectorTaskFactory<Boolean> isPidRunning(Integer pid) {
        return codePidRunning(pid).summary("PID "+pid+" is-running check (boolean)").returning(new Function<ProcessTaskWrapper<?>, Boolean>() {
            @Override
            public Boolean apply(@Nullable ProcessTaskWrapper<?> input) { return Integer.valueOf(0).equals(input.getExitCode()); }
        });
    }


    @Deprecated /** @deprecated  since 1.1 supply bash context */
    public static SshEffectorTaskFactory<Integer> codePidFromFileRunning(final String pidFile) {
        return codePidFromFileRunning(BashCommandsConfigurable.newInstance(), pidFile);
    }
    /** task which returns 0 if pid in the given file is running;
     * method accepts wildcards so long as they match a single file on the remote end
     * <p>
     * returns 1 if no matching file, 
     * 1 if matching file but no matching process,
     * and 2 if 2+ matching files */
    public static SshEffectorTaskFactory<Integer> codePidFromFileRunning(BashCommandsConfigurable bash, final String pidFile) {
        return ssh(bash.chain(
                // this fails, but isn't an error
                bash.requireTest("-f "+pidFile, "The PID file "+pidFile+" does not exist."),
                // this fails and logs an error picked up later
                bash.requireTest("`ls "+pidFile+" | wc -w` -eq 1", "ERROR: there are multiple matching PID files"),
                // this fails and logs an error picked up later
                bash.require("cat "+pidFile, "ERROR: the PID file "+pidFile+" cannot be read (permissions?)."),
                // finally check the process
                "ps -p `cat "+pidFile+"`")).summary("PID file "+pidFile+" is-running check (exit code)")
                .allowingNonZeroExitCode()
                .addCompletionListener(new Function<ProcessTaskWrapper<?>,Void>() {
                    @Override
                    public Void apply(ProcessTaskWrapper<?> input) {
                        if (input.getStderr().contains("ERROR:"))
                            throw new IllegalStateException("Invalid or inaccessible PID filespec: "+pidFile);
                        return null;
                    }
                });
    }

    @Deprecated /** @deprecated  since 1.1 supply bash context */
    public static SshEffectorTaskFactory<?> requirePidFromFileRunning(String pidFile) {
        return requirePidFromFileRunning(BashCommandsConfigurable.newInstance(), pidFile);
    }
    /** task which fails if the pid in the given file is not running (or if there is no such PID file);
     * method accepts wildcards so long as they match a single file on the remote end (fails if 0 or 2+ matching files) */
    public static SshEffectorTaskFactory<?> requirePidFromFileRunning(BashCommandsConfigurable bash, String pidFile) {
        return codePidFromFileRunning(bash, pidFile)
                .summary("PID file "+pidFile+" is-running check (required)")
                .requiringExitCodeZero("Process with PID from file "+pidFile+" is required to be running");
    }

    @Deprecated /** @deprecated  since 1.1 supply bash context */
    public static SshEffectorTaskFactory<Boolean> isPidFromFileRunning(String pidFile) {
        return isPidFromFileRunning(BashCommandsConfigurable.newInstance(), pidFile);
    }
    /** as {@link #codePidFromFileRunning(BashCommandsConfigurable, String)} but returning boolean */
    public static SshEffectorTaskFactory<Boolean> isPidFromFileRunning(BashCommandsConfigurable bash, String pidFile) {
        return codePidFromFileRunning(bash, pidFile).summary("PID file "+pidFile+" is-running check (boolean)").
                returning(new Function<ProcessTaskWrapper<?>, Boolean>() {
                    @Override
                    public Boolean apply(@Nullable ProcessTaskWrapper<?> input) { return ((Integer)0).equals(input.getExitCode()); }
                });
    }

    /** extracts the values for the main brooklyn.ssh.config.* config keys (i.e. those declared in ConfigKeys) 
     * as declared on the entity, and inserts them in a map using the unprefixed state, for ssh.
     * <p>
     * currently this is computed for each call, which may be wasteful, but it is reliable in the face of config changes.
     * we could cache the Map.  note that we do _not_ cache (or even own) the SshTool; 
     * the SshTool is created or re-used by the SshMachineLocation making use of these properties */
    @Beta
    public static Map<String, Object> getSshFlags(Entity entity, Location optionalLocation) {
        return getSshFlags(entity, optionalLocation==null ? null : optionalLocation.config());
    }

    public static Map<String, Object> getSshFlags(Entity entity, Configurable.ConfigurationSupport optionalExtraConfig) {
        Set<ConfigKey<?>> sshConfig = MutableSet.of();
        
        sshConfig.addAll(((EntityInternal)entity).config().findKeysPresent(ConfigPredicates.nameStartsWith(SshTool.BROOKLYN_CONFIG_KEY_PREFIX)));
        
        if (optionalExtraConfig!=null)
            sshConfig.addAll(optionalExtraConfig.findKeysPresent(ConfigPredicates.nameStartsWith(SshTool.BROOKLYN_CONFIG_KEY_PREFIX)));
        
        StringConfigMap globalConfig = ((EntityInternal)entity).getManagementContext().getConfig();
        sshConfig.addAll(globalConfig.findKeysDeclared(ConfigPredicates.nameStartsWith(SshTool.BROOKLYN_CONFIG_KEY_PREFIX)));
        
        Map<String, Object> result = Maps.newLinkedHashMap();
        for (ConfigKey<?> key : sshConfig) {
            /*
             * Rely on config in the right order:
             * entity config will be preferred over location, and location over global.
             * (We can also accept null entity and so combine with SshTasks.getSshFlags.)
             */
            
            Maybe<Object> mv = ((EntityInternal)entity).config().getRaw(key);
            Object v = null;
            if (v==null && mv.isPresent()) {
                v = entity.config().get(key);
            }
            
            if (v==null && mv.isAbsent() && optionalExtraConfig!=null) {
                if (optionalExtraConfig instanceof BrooklynObjectInternal.ConfigurationSupportInternal) {
                    mv = ((BrooklynObjectInternal.ConfigurationSupportInternal) optionalExtraConfig).getRaw(key);
                    v = optionalExtraConfig.get(key);
                } else {
                    mv = Maybe.of(optionalExtraConfig.findKeysPresent(k -> key.equals(key)).stream().findFirst().map(k -> optionalExtraConfig.get(k)));
                    v = mv.get();
                }
            }

            if (mv.isAbsent()) {
                v = globalConfig.getConfig(key);
            }
            
            result.put(ConfigUtils.unprefixedKey(SshTool.BROOKLYN_CONFIG_KEY_PREFIX, key).getName(), v);
        }
        return result;
    }

    private static void applySshFlags(ConfigBag config, Entity entity, Configurable.ConfigurationSupport machineConfig) {
        if (entity!=null) {
            if (!config.get(IGNORE_ENTITY_SSH_FLAGS)) {
                config.putIfAbsent(getSshFlags(entity, machineConfig));
            }
        }
    }

}
