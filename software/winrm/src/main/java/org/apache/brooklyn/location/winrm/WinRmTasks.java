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
package org.apache.brooklyn.location.winrm;

import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.api.mgmt.TaskFactory;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.system.ProcessTaskFactory;
import org.apache.brooklyn.util.net.Urls;

import java.util.Map;

public class WinRmTasks {
    public static ProcessTaskFactory<Integer> newWinrmExecTaskFactory(org.apache.brooklyn.location.winrm.WinRmMachineLocation winRmMachineLocation, String ...commands) {
        return new PlainWinRmExecTaskFactory<>(winRmMachineLocation, commands);
    }

    public static TaskFactory<?> installFromUrl(final ResourceUtils utils, final Map<String, ?> props, final WinRmMachineLocation location, final String url, final String destPath) {
        return new TaskFactory<TaskAdaptable<?>>() {
            @Override
            public TaskAdaptable<?> newTask() {
                return Tasks.<Void>builder().displayName("installing "+ Urls.getBasename(url)).description("installing "+url+" to "+destPath).body(new Runnable() {
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

    public static WinRmPutTaskFactory newWinrmPutTaskFactory(WinRmMachineLocation winRmMachineLocation, String remoteFile) {
        return newWinRmPutTaskFactory(winRmMachineLocation, true, remoteFile);
    }
    public static WinRmPutTaskFactory newWinRmPutTaskFactory(WinRmMachineLocation machine, final boolean useMachineConfig, String remoteFile) {
        return new WinRmPutTaskFactory(machine, remoteFile);
    }
}
