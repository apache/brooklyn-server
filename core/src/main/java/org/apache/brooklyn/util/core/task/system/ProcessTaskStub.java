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
package org.apache.brooklyn.util.core.task.system;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.ssh.internal.RemoteExecTaskConfigHelper;
import org.apache.brooklyn.util.text.Strings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class ProcessTaskStub {
    
    protected final List<String> commands = new ArrayList<>();
    /** null for localhost */
    protected MachineLocation machine;
    protected RemoteExecTaskConfigHelper.RemoteExecCapability remoteExecCapability;
    
    // config data
    protected String summary;
    protected Function<List<String>,List<String>> commandModifier;
    protected final ConfigBag config = ConfigBag.newInstance();
    
    public static enum ScriptReturnType { CUSTOM, EXIT_CODE, STDOUT_STRING, STDOUT_BYTES, STDERR_STRING, STDERR_BYTES }
    protected Function<ProcessTaskWrapper<?>, ?> returnResultTransformation = null;
    protected ScriptReturnType returnType = ScriptReturnType.EXIT_CODE;
    
    protected Boolean runAsScript = null;
    protected boolean runAsRoot = false;
    protected Boolean requireExitCodeZero = null;
    protected String extraErrorMessage = null;
    protected Map<String,String> shellEnvironment = new MutableMap<String, String>();
    protected final List<Function<ProcessTaskWrapper<?>, Void>> completionListeners = new ArrayList<Function<ProcessTaskWrapper<?>,Void>>();

    public ProcessTaskStub() {}

    protected ProcessTaskStub(ProcessTaskStub source) {
        commands.addAll(source.getCommands(false));
        machine = source.getMachine();
        remoteExecCapability = source.getRemoteExecCapability();
        summary = source.getSummary();
        config.copy(source.getConfig());
        commandModifier = source.commandModifier;
        returnResultTransformation = source.returnResultTransformation;
        returnType = source.returnType;
        runAsScript = source.runAsScript;
        runAsRoot = source.runAsRoot;
        requireExitCodeZero = source.requireExitCodeZero;
        extraErrorMessage = source.extraErrorMessage;
        shellEnvironment.putAll(source.getShellEnvironment());
        completionListeners.addAll(source.getCompletionListeners());
    }

    public String getSummary() {
        if (summary!=null) return summary;
        return Strings.maxlen(Strings.join(commands, " ; "), 160);
    }
    
    /** null for localhost */
    public MachineLocation getMachine() {
        return machine;
    }
    public RemoteExecTaskConfigHelper.RemoteExecCapability getRemoteExecCapability() {
        if (remoteExecCapability!=null) return remoteExecCapability;
        if (machine!=null) {
            remoteExecCapability = new RemoteExecTaskConfigHelper.RemoteExecCapabilityFromLocation(machine);
            return remoteExecCapability;
        }
        return null;
    }
    
    public Map<String, String> getShellEnvironment() {
        return ImmutableMap.copyOf(shellEnvironment);
    }
 
    @Override
    public String toString() {
        return super.toString()+"["+getSummary()+"]";
    }

    public List<String> getCommands() {
        return getCommands(false);
    }
    public List<String> getCommands(boolean modified) {
        return ImmutableList.copyOf(modified && commandModifier!=null ? commandModifier.apply(commands) : commands);
    }
 
    public List<Function<ProcessTaskWrapper<?>, Void>> getCompletionListeners() {
        return completionListeners;
    }

    protected ConfigBag getConfig() { return config; }
    
}
