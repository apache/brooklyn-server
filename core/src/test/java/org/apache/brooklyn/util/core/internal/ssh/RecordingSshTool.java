/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/
package org.apache.brooklyn.util.core.internal.ssh;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * For stubbing out the {@link SshTool}, so that no real ssh/scp commands are executed.
 * Records all the commands that are executed, so that assertions can subsequently be made.
 * 
 * By default, all commands return exit code 0, and no stdout/stderr.
 * 
 * This can be customised for particular commands using {@link #setCustomResponse(String, CustomResponseGenerator)}
 * to specify the exit code, stdout and stderr of a matching command.
 */
public class RecordingSshTool implements SshTool {
    
    public static class ExecParams {
        public final Map<String, ?> props;
        public final List<String> commands;
        public final Map<String, ?> env;
        
        public ExecParams(Map<String, ?> props, List<String> commands, Map<String, ?> env) {
            this.props = props;
            this.commands = commands;
            this.env = env;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("props", props)
                    .add("commands", commands)
                    .add("env", env).toString();
        }
    }

    public interface CustomResponseGenerator {
        public CustomResponse generate(ExecParams execParams) throws Exception;
    }

    public static class CustomResponse {
        public final int exitCode;
        public final String stdout;
        public final String stderr;
        
        public CustomResponse(int exitCode, String stdout, String stderr) {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }
        
        @Override
        public String toString() {
            return "CustomResponse["+exitCode+"; "+stdout+"; "+stderr+"]";
        }
        
        public CustomResponseGenerator toGenerator() {
            return new CustomResponseGenerator() {
                @Override public CustomResponse generate(ExecParams execParams) {
                    return CustomResponse.this;
                }
            };
        }
    }
    
    public static class ExecCmd {
        public final Map<String,?> props;
        public final String summaryForLogging;
        public final List<String> commands;
        public final Map<?,?> env;
        
        ExecCmd(Map<String,?> props, String summaryForLogging, List<String> commands, Map<?,?> env) {
            this.props = props;
            this.summaryForLogging = summaryForLogging;
            this.commands = commands;
            this.env = env;
        }
        
        @Override
        public String toString() {
            return "ExecCmd["+summaryForLogging+": "+commands+"; "+props+"; "+env+"]";
        }
    }
    
    public static class ExecCmdPredicates {
        public static Predicate<ExecCmd> containsEnv(final Map<String, ?> expected) {
            return new Predicate<ExecCmd>() {
                @Override public boolean apply(@Nullable ExecCmd input) {
                    if (input == null) return false;
                    if (input.env == null) return false;
                    for (Map.Entry<?,?> entry : expected.entrySet()) {
                        Object key = entry.getKey();
                        if (!(input.env.containsKey(key) && Objects.equal(entry.getValue(), input.env.get(key)))) {
                            return false;
                        }
                    }
                    return true;
                }};
        }
        public static Predicate<ExecCmd> containsCmd(final String expected) {
            return new Predicate<ExecCmd>() {
                @Override public boolean apply(@Nullable ExecCmd input) {
                    if (input == null) return false;
                    for (String cmd : input.commands) {
                        if (expected.equals(cmd)) {
                            return true;
                        }
                    }
                    return false;
                }};
        }
    }
    
    public static List<ExecCmd> execScriptCmds = Lists.newCopyOnWriteArrayList();
    public static List<Map<?,?>> constructorProps = Lists.newCopyOnWriteArrayList();
    public static Map<String, CustomResponseGenerator> customResponses = Maps.newConcurrentMap();
    
    private boolean connected;
    
    public static void clear() {
        execScriptCmds.clear();
        constructorProps.clear();
        customResponses.clear();
    }
    
    public static void clearCmdHistory() {
        execScriptCmds.clear();
    }

    public static void setCustomResponse(String cmdRegex, CustomResponseGenerator response) {
        customResponses.put(cmdRegex, checkNotNull(response, "response"));
    }
    
    public static void setCustomResponse(String cmdRegex, CustomResponse response) {
        customResponses.put(cmdRegex, checkNotNull(response, "response").toGenerator());
    }
    
    public static List<ExecCmd> getExecCmds() {
        return ImmutableList.copyOf(execScriptCmds);
    }
    
    public static ExecCmd getLastExecCmd() {
        return execScriptCmds.get(execScriptCmds.size()-1);
    }
    
    public static Map<?,?> getLastConstructorProps() {
        return constructorProps.get(constructorProps.size()-1);
    }
    
    public RecordingSshTool(Map<?,?> props) {
        constructorProps.add(props);
    }
    @Override public void connect() {
        connected = true;
    }
    @Override public void connect(int maxAttempts) {
        connected = true;
    }
    @Override public void disconnect() {
        connected = false;
    }
    @Override public boolean isConnected() {
        return connected;
    }
    @Override public int execScript(Map<String, ?> props, List<String> commands, Map<String, ?> env) {
        return execInternal(props, commands, env);
    }
    @Override public int execCommands(Map<String, ?> props, List<String> commands, Map<String, ?> env) {
        return execInternal(props, commands, env);
    }
    @Override public int execScript(Map<String, ?> props, List<String> commands) {
        return execScript(props, commands, ImmutableMap.<String,Object>of());
    }
    @Override public int execCommands(Map<String, ?> props, List<String> commands) {
        return execCommands(props, commands, ImmutableMap.<String,Object>of());
    }
    @Override public int copyToServer(Map<String, ?> props, File localFile, String pathAndFileOnRemoteServer) {
        return 0;
    }
    @Override public int copyToServer(Map<String, ?> props, InputStream contents, String pathAndFileOnRemoteServer) {
        return 0;
    }
    @Override public int copyToServer(Map<String, ?> props, byte[] contents, String pathAndFileOnRemoteServer) {
        return 0;
    }
    @Override public int copyFromServer(Map<String, ?> props, String pathAndFileOnRemoteServer, File local) {
        return 0;
    }
    protected int execInternal(Map<String, ?> props, List<String> commands, Map<String, ?> env) {
        execScriptCmds.add(new ExecCmd(props, "", commands, env));
        for (String cmd : commands) {
            for (Entry<String, CustomResponseGenerator> entry : customResponses.entrySet()) {
                if (cmd.matches(entry.getKey())) {
                    CustomResponseGenerator responseGenerator = entry.getValue();
                    CustomResponse response;
                    try {
                        response = responseGenerator.generate(new ExecParams(props, commands, env));
                    } catch (Exception e) {
                        throw Exceptions.propagate(e);
                    }
                    writeCustomResponseStreams(props, response);
                    return response.exitCode;
                }
            }
        }
        return 0;
    }
    protected void writeCustomResponseStreams(Map<String, ?> props, CustomResponse response) {
        try {
            if (Strings.isNonBlank(response.stdout) && props.get(SshTool.PROP_OUT_STREAM.getName()) != null) {
                ((OutputStream)props.get(SshTool.PROP_OUT_STREAM.getName())).write(response.stdout.getBytes());
            }
            if (Strings.isNonBlank(response.stderr) && props.get(SshTool.PROP_ERR_STREAM.getName()) != null) {
                ((OutputStream)props.get(SshTool.PROP_ERR_STREAM.getName())).write(response.stderr.getBytes());
            }
        } catch (IOException e) {
            Exceptions.propagate(e);
        }
    }
}
