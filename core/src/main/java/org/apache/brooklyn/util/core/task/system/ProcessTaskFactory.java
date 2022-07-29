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

import com.google.common.annotations.Beta;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.apache.brooklyn.util.core.task.system.ProcessTaskStub.ScriptReturnType;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public interface ProcessTaskFactory<T> extends SimpleProcessTaskFactory<ProcessTaskFactory<T>,ProcessTaskWrapper<?>,T,ProcessTaskWrapper<T>> {
    ProcessTaskFactory<T> machine(MachineLocation machine);
    ProcessTaskFactory<T> add(String ...commandsToAdd);
    ProcessTaskFactory<T> add(Iterable<String> commandsToAdd);
    ProcessTaskFactory<T> requiringExitCodeZero();
    ProcessTaskFactory<T> requiringExitCodeZero(String extraErrorMessage);
    @Override ProcessTaskFactory<T> allowingNonZeroExitCode();

    ProcessTaskFactory<String> requiringZeroAndReturningStdout();
    ProcessTaskFactory<Boolean> returningIsExitCodeZero();
    <RET2> ProcessTaskFactory<RET2> returning(ScriptReturnType type);
    @Override ProcessTaskFactory<String> returningStdout();
    @Override ProcessTaskFactory<Integer> returningExitCodeAllowingNonZero();
    @Override <TT2> ProcessTaskFactory<TT2> returning(Function<ProcessTaskWrapper<?>, TT2> resultTransformation);

    //public <RET2> ProcessTaskFactory<T0,RET2> returning(Function<ProcessTaskWrapper<T0>, RET2> resultTransformation);
    ProcessTaskFactory<T> runAsCommand();
    ProcessTaskFactory<T> runAsScript();
    ProcessTaskFactory<T> runAsRoot();
    ProcessTaskFactory<T> commandModifier(Function<List<String>, List<String>> modifier);
    @Override ProcessTaskFactory<T> environmentVariable(String key, String val);
    @Override ProcessTaskFactory<T> environmentVariables(Map<String,String> vars);
    @Override ProcessTaskFactory<T> summary(String summary);
    
    /** allows setting config-key based properties for specific underlying tools */
    @Beta
    <V> ProcessTaskFactory<T> configure(ConfigKey<V> key, V value);

    /** allows setting config-key/flag based properties for specific underlying tools;
     * but note that if any are prefixed with {@link SshTool#BROOKLYN_CONFIG_KEY_PREFIX}
     * these should normally be filtered out */
    @Beta
    ProcessTaskFactory<T> configure(Map<?,?> flags);

    /** adds a listener which will be notified of (otherwise) successful completion,
     * typically used to invalidate the result (ie throw exception, to promote a string in the output to an exception);
     * invoked even if return code is zero, so a better error can be thrown */
    ProcessTaskFactory<T> addCompletionListener(Function<ProcessTaskWrapper<?>, Void> function);
}
