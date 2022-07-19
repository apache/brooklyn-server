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

import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.api.mgmt.TaskFactory;

import java.util.Map;
import java.util.function.Function;

public interface SimpleProcessTaskFactory<SELF extends SimpleProcessTaskFactory<SELF,T0,T,TT>,T0,T,TT extends TaskAdaptable<T>> extends TaskFactory<TT> {

    SELF summary(String summary);

    SELF environmentVariable(String key, String val);
    SELF environmentVariables(Map<String,String> vars);

    SELF allowingNonZeroExitCode();

    SimpleProcessTaskFactory<?,T0,String,?> returningStdout();
    SimpleProcessTaskFactory<?,T0,Integer,?> returningExitCodeAllowingNonZero();

    <T2> SimpleProcessTaskFactory<?,T0,T2,?> returning(Function<T0, T2> resultTransformation);

}
