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
package org.apache.brooklyn.util.core.internal.winrm;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.brooklyn.util.stream.Streams;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * For stubbing out the {@link WinRmTool}, so that no real winrm commands are executed.
 * Records all the commands that are executed, so that assertions can subsequently be made.
 * 
 * By default, all commands return exit code 0, and no stdout/stderr.
 */
public class RecordingWinRmTool implements WinRmTool {

    public enum ExecType {
        COMMAND,
        POWER_SHELL,
        COPY_TO_SERVER;
    }
    
    public static class ExecParams {
        public final ExecType type;
        public final Map<?, ?> constructorProps;
        public final List<String> commands;
        
        public ExecParams(ExecType type, Map<?, ?> constructorProps, List<String> commands) {
            this.type = type;
            this.constructorProps = constructorProps;
            this.commands = commands;
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("type", type)
                    .add("constructorProps", constructorProps)
                    .add("commands", commands)
                    .toString();
        }
    }

    public interface CustomResponseGenerator {
        public CustomResponse generate(ExecParams execParams);
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
    
    public static List<ExecParams> execs = Lists.newCopyOnWriteArrayList();
    public static List<Map<?,?>> constructorProps = Lists.newCopyOnWriteArrayList();
    public static Map<String, CustomResponseGenerator> customResponses = Collections.synchronizedMap(new LinkedHashMap<>());
    
    public static void clear() {
        execs.clear();
        constructorProps.clear();
        customResponses.clear();
    }
    
    public static void setCustomResponse(String cmdRegex, CustomResponseGenerator response) {
        customResponses.put(cmdRegex, checkNotNull(response, "response"));
    }
    
    public static void setCustomResponse(String cmdRegex, CustomResponse response) {
        customResponses.put(cmdRegex, checkNotNull(response, "response").toGenerator());
    }
    
    public static List<ExecParams> getExecs() {
        return ImmutableList.copyOf(execs);
    }
    
    public static ExecParams getLastExec() {
        if (execs.isEmpty()) {
            throw new IllegalStateException("No executions recorded");
        }
        return execs.get(execs.size()-1);
    }

    public static Map<?,?> getLastConstructorProps() {
        return constructorProps.get(constructorProps.size()-1);
    }

    private final Map<?, ?> ownConstructorProps;
    
    public RecordingWinRmTool(Map<?,?> props) {
        constructorProps.add(props);
        ownConstructorProps = props;
    }
    
    @Override
    public WinRmToolResponse executeCommand(List<String> commands) {
        ExecParams execParams = new ExecParams(ExecType.COMMAND, ownConstructorProps, commands);
        execs.add(execParams);
        return generateResponse(execParams);
    }

    @Override
    public WinRmToolResponse executePs(List<String> commands) {
        ExecParams execParams = new ExecParams(ExecType.POWER_SHELL, ownConstructorProps, commands);
        execs.add(execParams);
        return generateResponse(execParams);
    }

    @Override
    public WinRmToolResponse copyToServer(InputStream source, String destination) {
        execs.add(new ExecParams(ExecType.COPY_TO_SERVER, ownConstructorProps, ImmutableList.of(new String(Streams.readFully(source)))));
        return new WinRmToolResponse("", "", 0);
    }

    @Override
    @Deprecated
    public WinRmToolResponse executeScript(List<String> commands) {
        throw new UnsupportedOperationException();
    }
    
    protected WinRmToolResponse generateResponse(ExecParams execParams) {
        LinkedHashMap<String, CustomResponseGenerator> customResponsesCopy;
        synchronized (customResponses) {
            customResponsesCopy = new LinkedHashMap<>(customResponses);
        }
        for (String cmd : execParams.commands) {
            for (Entry<String, CustomResponseGenerator> entry : customResponsesCopy.entrySet()) {
                if (cmd.matches(entry.getKey())) {
                    CustomResponseGenerator responseGenerator = entry.getValue();
                    CustomResponse response = responseGenerator.generate(execParams);
                    return new WinRmToolResponse(response.stdout, response.stderr, response.exitCode);
                }
            }
        }
        return new WinRmToolResponse("", "", 0);
    }
}
