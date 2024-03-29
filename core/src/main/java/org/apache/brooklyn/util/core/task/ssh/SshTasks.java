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
package org.apache.brooklyn.util.core.task.ssh;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.*;
import org.apache.brooklyn.api.objs.Configurable;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.StringConfigMap;
import org.apache.brooklyn.core.config.ConfigPredicates;
import org.apache.brooklyn.core.config.ConfigUtils;
import org.apache.brooklyn.core.effector.ssh.SshEffectorTasks;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.file.BrooklynOsCommands;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ssh.internal.PlainSshExecTaskFactory;
import org.apache.brooklyn.util.core.task.ssh.internal.RemoteExecTaskConfigHelper;
import org.apache.brooklyn.util.core.task.system.ProcessTaskFactory;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.ssh.BashCommandsConfigurable;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Conveniences for generating {@link Task} instances to perform SSH activities on an {@link SshMachineLocation}.
 * <p>
 * To infer the {@link SshMachineLocation} and take properties from entities and global management context the
 * {@link SshEffectorTasks} should be preferred over this class.
 *  
 * @see SshEffectorTasks
 * @since 0.6.0
 */
@Beta
public class SshTasks {

    private static final Logger log = LoggerFactory.getLogger(SshTasks.class);

    public static ProcessTaskFactory<Integer> newSshExecTaskFactory(SshMachineLocation machine, String ...commands) {
        return newSshExecTaskFactory(machine,  Arrays.asList(commands));
    }

    public static ProcessTaskFactory<Integer> newSshExecTaskFactory(RemoteExecTaskConfigHelper.RemoteExecCapability config, String ...commands) {
        return newSshExecTaskFactory(config,  Arrays.asList(commands));
    }

    public static ProcessTaskFactory<Integer> newSshExecTaskFactory(SshMachineLocation machine, final boolean useMachineConfig, String ...commands) {
        return newSshExecTaskFactory(machine, useMachineConfig, Arrays.asList(commands));
    }

    public static ProcessTaskFactory<Integer> newSshExecTaskFactory(SshMachineLocation machine, List<String> commands) {
        return newSshExecTaskFactory(machine, true, commands);
    }

    public static ProcessTaskFactory<Integer> newSshExecTaskFactory(RemoteExecTaskConfigHelper.RemoteExecCapability config, List<String> commands) {
        return newSshExecTaskFactory(config, true, commands);
    }

    public static ProcessTaskFactory<Integer> newSshExecTaskFactory(SshMachineLocation machine, final boolean useMachineConfig, List<String> commands) {
        return new PlainSshExecTaskFactory<Integer>(machine, commands) {
            {
                if (useMachineConfig)
                    config.putIfAbsent(getSshFlags(machine));
            }
        };
    }

    public static ProcessTaskFactory<Integer> newSshExecTaskFactory(RemoteExecTaskConfigHelper.RemoteExecCapability remoteExecCapability, final boolean useMachineConfig, List<String> commands) {
        return new PlainSshExecTaskFactory<Integer>(remoteExecCapability, commands) {
            {
                if (useMachineConfig)
                    config.putIfAbsent(getSshFlags(remoteExecCapability.getManagementContext(), remoteExecCapability.getExtraConfiguration()));
            }
        };
    }

    public static SshPutTaskFactory newSshPutTaskFactory(SshMachineLocation machine, String remoteFile) {
        return newSshPutTaskFactory(machine, true, remoteFile);
    }
    
    public static SshPutTaskFactory newSshPutTaskFactory(SshMachineLocation machine, final boolean useMachineConfig, String remoteFile) {
        return new SshPutTaskFactory(machine, remoteFile) {
            {
                if (useMachineConfig)
                    config.putIfAbsent(getSshFlags(machine));
            }
        };
    }

    public static SshFetchTaskFactory newSshFetchTaskFactory(SshMachineLocation machine, String remoteFile) {
        return newSshFetchTaskFactory(machine, true, remoteFile);
    }
    
    public static SshFetchTaskFactory newSshFetchTaskFactory(SshMachineLocation machine, final boolean useMachineConfig, String remoteFile) {
        return new SshFetchTaskFactory(machine, remoteFile) {
            {
                if (useMachineConfig)
                    config.putIfAbsent(getSshFlags(machine));
            }
        };
    }

    private static Map<String, Object> getSshFlags(Location location) {
        return getSshFlags(location instanceof AbstractLocation ? ((AbstractLocation)location).getManagementContext() : null, location.config());
    }
    private static Map<String, Object> getSshFlags(ManagementContext mgmt, Configurable.ConfigurationSupport config) {
        Set<ConfigKey<?>> sshConfig = MutableSet.of();
        
        StringConfigMap mgmtConfig = null;
        sshConfig.addAll(config.findKeysPresent(ConfigPredicates.nameStartsWith(SshTool.BROOKLYN_CONFIG_KEY_PREFIX)));

        if (mgmt!=null) {
            if (mgmt!=null) {
                mgmtConfig = mgmt.getConfig();
                sshConfig.addAll(mgmtConfig.findKeysPresent(ConfigPredicates.nameStartsWith(SshTool.BROOKLYN_CONFIG_KEY_PREFIX)));
            }
        }
        
        
        Map<String, Object> result = Maps.newLinkedHashMap();
        for (ConfigKey<?> key : sshConfig) {
            // cf in SshEffectorTasks

            Maybe<Object> v;
            if (config instanceof BrooklynObjectInternal.ConfigurationSupportInternal) {
                v = ((BrooklynObjectInternal.ConfigurationSupportInternal) config).getRaw(key);
            } else {
                v = Maybe.of(config.findKeysPresent(k -> key.equals(key)).stream().findFirst().map(k -> config.get(k)));
            }

            if (v.isAbsent() && mgmtConfig!=null) v = Maybe.of( mgmtConfig.getConfig(key) );
            if (!v.isAbsent()) result.put(ConfigUtils.unprefixedKey(SshTool.BROOKLYN_CONFIG_KEY_PREFIX, key).getName(), v.get());
        }
        return result;
    }

    @Beta
    public static enum OnFailingTask { 
        FAIL,
        /** issues a warning, sometimes implemented as marking the task inessential and failing it if it appears
         * we are in a dynamic {@link TaskQueueingContext};
         * useful because this way the warning appears to the user;
         * but note that the check is done against the calling thread so use with some care
         * (and thus this enum is currently here rather then elsewhere) */
        WARN_OR_IF_DYNAMIC_FAIL_MARKING_INESSENTIAL,
        /** issues a warning in the log if the task fails, otherwise swallows it */
        WARN_IN_LOG_ONLY, 
        /** not even a warning if the task fails (the caller is expected to handle it as appropriate) */
        IGNORE }
    
    public static ProcessTaskFactory<Boolean> dontRequireTtyForSudo(SshMachineLocation machine, final boolean failIfCantSudo) {
        return dontRequireTtyForSudo(machine, failIfCantSudo ? OnFailingTask.FAIL : OnFailingTask.WARN_IN_LOG_ONLY);
    }
    /** creates a task which returns modifies sudoers to ensure non-tty access is permitted;
     * also gives nice warnings if sudo is not permitted */
    public static ProcessTaskFactory<Boolean> dontRequireTtyForSudo(SshMachineLocation machine, OnFailingTask onFailingTaskRequested) {
        final OnFailingTask onFailingTask;
        if (onFailingTaskRequested==OnFailingTask.WARN_OR_IF_DYNAMIC_FAIL_MARKING_INESSENTIAL) {
            if (DynamicTasks.getTaskQueuingContext()!=null)
                onFailingTask = onFailingTaskRequested;
            else
                onFailingTask = OnFailingTask.WARN_IN_LOG_ONLY;
        } else {
            onFailingTask = onFailingTaskRequested;
        }
        
        final String id = Identifiers.makeRandomId(6);
        return newSshExecTaskFactory(machine, "(commands to modify sudo config to allow tty)")
                .commandModifier(x -> {
                    Entity entity = BrooklynTaskTags.getTargetOrContextEntity(Tasks.current());
                    BashCommandsConfigurable bash;
                    if (entity!=null) bash = BrooklynOsCommands.bash(machine);
                    else bash = BrooklynOsCommands.bash(machine);
                    return MutableList.of(
                        bash.dontRequireTtyForSudo(),
                        // strange quotes are to ensure we don't match against echoed stdin
                        bash.sudo("echo \"sudo\"-is-working-"+id));
            })
            .summary("patch /etc/sudoers to disable requiretty")
            .configure(SshTool.PROP_ALLOCATE_PTY, true)
            .allowingNonZeroExitCode()
            .returning(new Function<ProcessTaskWrapper<?>,Boolean>() { @Override public Boolean apply(ProcessTaskWrapper<?> task) {
                if (task.getExitCode()==0 && task.getStdout().contains("sudo-is-working-"+id)) return true;
                Entity entity = BrooklynTaskTags.getTargetOrContextEntity(Tasks.current());
                
                
                if (onFailingTask!=OnFailingTask.IGNORE) {
                    // TODO if in a queueing context can we mark this task inessential and throw?
                    // that way user sees the message...
                    String message = "Error setting up sudo for "+task.getRemoteExecCapability().getConnectionSummary()+" "+
                        " (exit code "+task.getExitCode()+(entity!=null ? ", entity "+entity : "")+")";
                    DynamicTasks.queueIfPossible(Tasks.warning(message, null));
                }
                Streams.logStreamTail(log, "STDERR of sudo setup problem", Streams.byteArrayOfString(task.getStderr()), 1024);
                
                if (onFailingTask==OnFailingTask.WARN_OR_IF_DYNAMIC_FAIL_MARKING_INESSENTIAL) {
                    Tasks.markInessential();
                }
                if (onFailingTask==OnFailingTask.FAIL || onFailingTask==OnFailingTask.WARN_OR_IF_DYNAMIC_FAIL_MARKING_INESSENTIAL) {
                    throw new IllegalStateException("Passwordless sudo is required for "+task.getRemoteExecCapability().getConnectionSummary()+
                            (entity!=null ? " ("+entity+")" : ""));
                }
                return false; 
            } });
    }

    /** Function for use in {@link ProcessTaskFactory#returning(Function)} which logs all information, optionally requires zero exit code, 
     * and then returns stdout */
    public static Function<ProcessTaskWrapper<?>, String> returningStdoutLoggingInfo(final Logger logger, final boolean requireZero) {
        return new Function<ProcessTaskWrapper<?>, String>() {
          @Override
          public String apply(@Nullable ProcessTaskWrapper<?> input) {
            if (logger!=null) logger.info(input+" COMMANDS:\n"+Strings.join(input.getCommands(true),"\n"));
            if (logger!=null) logger.info(input+" STDOUT:\n"+input.getStdout());
            if (logger!=null) logger.info(input+" STDERR:\n"+input.getStderr());
            if (requireZero && input.getExitCode()!=0) 
                throw new IllegalStateException("non-zero exit code in "+input.getSummary()+": see log for more details!");
            return input.getStdout();
          }
        };
    }

    /** task to install a file given a url, where the url is resolved remotely first then locally */
    public static TaskFactory<?> installFromUrl(final SshMachineLocation location, final String url, final String destPath) {
        return installFromUrl(ResourceUtils.create(SshTasks.class), ImmutableMap.<String,Object>of(), location, url, destPath);
    }
    /** task to install a file given a url, where the url is resolved remotely first then locally */
    public static TaskFactory<?> installFromUrl(final Map<String, ?> props, final SshMachineLocation location, final String url, final String destPath) {
        return installFromUrl(ResourceUtils.create(SshTasks.class), props, location, url, destPath);
    }

    /** task to install a file given a url, where the url is resolved remotely first then locally */
    public static TaskFactory<?> installFromUrl(final ResourceUtils utils, final Map<String, ?> props, final SshMachineLocation location, final String url, final String destPath) {
        return new TaskFactory<TaskAdaptable<?>>() {
            @Override
            public TaskAdaptable<?> newTask() {
                return Tasks.<Void>builder().displayName("installing "+Urls.getBasename(url)).description("installing "+url+" to "+destPath).body(new Runnable() {
                    @Override
                    public void run() {
                        int result = location.installTo(utils, props, url, destPath);
                        if (result!=0) 
                            throw new IllegalStateException("Failed to install '"+url+"' to '"+destPath+"' at "+location+": exit code "+result);
                    }
                }).build();
            }
        };
    }
    
}
