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
package org.apache.brooklyn.util.core.task.system.internal;

import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.internal.ssh.ShellAbstractTool;
import org.apache.brooklyn.util.core.internal.ssh.ShellTool;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.stream.LoggingOutputStream;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;

import com.google.common.base.Function;

public abstract class ExecWithLoggingHelpers {

    public static final ConfigKey<OutputStream> STDOUT = SshMachineLocation.STDOUT;
    public static final ConfigKey<OutputStream> STDERR = SshMachineLocation.STDERR;
    public static final ConfigKey<Boolean> NO_STDOUT_LOGGING = SshMachineLocation.NO_STDOUT_LOGGING;
    public static final ConfigKey<Boolean> NO_STDERR_LOGGING = SshMachineLocation.NO_STDERR_LOGGING;
    public static final ConfigKey<String> LOG_PREFIX = SshMachineLocation.LOG_PREFIX;

    protected final String shortName;
    protected Logger commandLogger = null;
    
    public interface ExecRunner {
        public int exec(ShellTool ssh, Map<String,?> flags, List<String> cmds, Map<String,?> env);
    }

    protected abstract <T> T execWithTool(MutableMap<String, Object> toolCreationAndConnectionProperties, Function<ShellTool, T> runMethodOnTool);
    protected abstract void preExecChecks();
    protected abstract String getTargetName();
    protected abstract String constructDefaultLoggingPrefix(ConfigBag execFlags);

    /** takes a very short name for use in blocking details, e.g. SSH or Process */
    public ExecWithLoggingHelpers(String shortName) {
        this.shortName = shortName;
    }

    public ExecWithLoggingHelpers logger(Logger commandLogger) {
        this.commandLogger = commandLogger;
        return this;
    }
    
    public int execScript(Map<String,?> props, String summaryForLogging, List<String> commands, Map<String,?> env) {
        // TODO scriptHeader are the extra commands we expect the SshTool/ShellTool to add.
        // Would be better if could get this from the ssh-tool, rather than assuming it will behave as
        // we expect.
        String scriptHeader = ShellAbstractTool.getOptionalVal(props, ShellTool.PROP_SCRIPT_HEADER);
        
        return execWithLogging(props, summaryForLogging, commands, env, scriptHeader, new ExecRunner() {
                @Override public int exec(ShellTool ssh, Map<String, ?> flags, List<String> cmds, Map<String, ?> env) {
                    return ssh.execScript(flags, cmds, env);
                }});
    }

    protected static <T> T getOptionalVal(Map<String,?> map, ConfigKey<T> keyC) {
        if (keyC==null) return null;
        String key = keyC.getName();
        if (map!=null && map.containsKey(key)) {
            return TypeCoercions.coerce(map.get(key), keyC.getTypeToken());
        } else {
            return keyC.getDefaultValue();
        }
    }

    public int execCommands(Map<String,?> props, String summaryForLogging, List<String> commands, Map<String,?> env) {
        return execWithLogging(props, summaryForLogging, commands, env, new ExecRunner() {
                @Override public int exec(ShellTool tool, Map<String,?> flags, List<String> cmds, Map<String,?> env) {
                    return tool.execCommands(flags, cmds, env);
                }});
    }

    public int execWithLogging(Map<String,?> props, final String summaryForLogging, final List<String> commands,
            final Map<String,?> env, final ExecRunner execCommand) {
        return execWithLogging(props, summaryForLogging, commands, env, null, execCommand);
    }
    
    public int execWithLogging(Map<String,?> props, final String summaryForLogging, final List<String> commands,
            final Map<String,?> env, String expectedCommandHeaders, final ExecRunner execCommand) {
        if (commandLogger!=null && commandLogger.isDebugEnabled()) {
            String allcmds = (Strings.isBlank(expectedCommandHeaders) ? "" : expectedCommandHeaders + " ; ") + Strings.join(commands, " ; ");
            commandLogger.debug("{}, initiating "+shortName.toLowerCase()+" on machine {}{}: {}",
                    new Object[] {summaryForLogging, getTargetName(),
                    env!=null && !env.isEmpty() ? " (env "+Sanitizer.sanitize(env)+")": "", allcmds});
        }

        if (commands.isEmpty()) {
            if (commandLogger!=null && commandLogger.isDebugEnabled())
                commandLogger.debug("{}, on machine {}, ending: no commands to run", summaryForLogging, getTargetName());
            return 0;
        }

        final ConfigBag execFlags = new ConfigBag().putAll(props);
        // some props get overridden in execFlags, so remove them from the tool flags
        final ConfigBag toolFlags = new ConfigBag().putAll(props).removeAll(
                LOG_PREFIX, STDOUT, STDERR, ShellTool.PROP_NO_EXTRA_OUTPUT);

        execFlags.configure(ShellTool.PROP_SUMMARY, summaryForLogging);
        
        preExecChecks();
        
        String logPrefix = execFlags.get(LOG_PREFIX);
        if (logPrefix==null) logPrefix = constructDefaultLoggingPrefix(execFlags);

        if (!execFlags.get(NO_STDOUT_LOGGING)) {
            String stdoutLogPrefix = "["+(logPrefix != null ? logPrefix+":stdout" : "stdout")+"] ";
            OutputStream outO = LoggingOutputStream.builder()
                    .outputStream(execFlags.get(STDOUT))
                    .logger(commandLogger)
                    .logPrefix(stdoutLogPrefix)
                    .build();

            execFlags.put(STDOUT, outO);
        }

        if (!execFlags.get(NO_STDERR_LOGGING)) {
            String stderrLogPrefix = "["+(logPrefix != null ? logPrefix+":stderr" : "stderr")+"] ";
            OutputStream outE = LoggingOutputStream.builder()
                    .outputStream(execFlags.get(STDERR))
                    .logger(commandLogger)
                    .logPrefix(stderrLogPrefix)
                    .build();
            execFlags.put(STDERR, outE);
        }

        Tasks.setBlockingDetails(shortName+" executing, "+summaryForLogging);
        try {
            return execWithTool(MutableMap.copyOf(toolFlags.getAllConfig()), new Function<ShellTool, Integer>() {
                @Override
                public Integer apply(ShellTool tool) {
                    int result = execCommand.exec(tool, MutableMap.copyOf(execFlags.getAllConfig()), commands, env);
                    if (commandLogger!=null && commandLogger.isDebugEnabled()) 
                        commandLogger.debug("{}, on machine {}, completed: return status {}",
                                new Object[] {summaryForLogging, getTargetName(), result});
                    return result;
                }});

        } finally {
            Tasks.setBlockingDetails(null);
        }
    }

}
