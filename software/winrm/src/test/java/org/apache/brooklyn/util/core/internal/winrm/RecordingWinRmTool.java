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

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.util.stream.Streams;

import com.google.common.base.Objects;
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
        public final List<String> commands;
        
        public ExecParams(ExecType type, List<String> commands) {
            this.type = type;
            this.commands = commands;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("type", type)
                    .add("commands", commands)
                    .toString();
        }
    }

    
    public static List<ExecParams> execs = Lists.newCopyOnWriteArrayList();
    public static List<Map<?,?>> constructorProps = Lists.newCopyOnWriteArrayList();
    
    public static void clear() {
        execs.clear();
        constructorProps.clear();
    }
    
    public static List<ExecParams> getExecs() {
        return ImmutableList.copyOf(execs);
    }
    
    public static ExecParams getLastExec() {
        return execs.get(execs.size()-1);
    }
    
    public RecordingWinRmTool(Map<?,?> props) {
        constructorProps.add(props);
    }
    
    @Override
    public WinRmToolResponse executeCommand(List<String> commands) {
        execs.add(new ExecParams(ExecType.COMMAND, commands));
        return new WinRmToolResponse("", "", 0);
    }

    @Override
    public WinRmToolResponse executePs(List<String> commands) {
        execs.add(new ExecParams(ExecType.POWER_SHELL, commands));
        return new WinRmToolResponse("", "", 0);
    }

    @Override
    public WinRmToolResponse copyToServer(InputStream source, String destination) {
        execs.add(new ExecParams(ExecType.COPY_TO_SERVER, ImmutableList.of(new String(Streams.readFully(source)))));
        return new WinRmToolResponse("", "", 0);
    }

    @Override
    @Deprecated
    public WinRmToolResponse executeScript(List<String> commands) {
        throw new UnsupportedOperationException();
    }
}
