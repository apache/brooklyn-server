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

import com.google.common.base.Supplier;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.ssh.SshPutTaskStub;
import org.apache.brooklyn.util.core.task.ssh.internal.RemoteExecTaskConfigHelper;

import java.io.InputStream;

public class WinRmPutTaskStub {
    protected String remoteFile;
    protected WinRmMachineLocation machine;
    protected RemoteExecTaskConfigHelper.RemoteExecCapability remoteExecCapability;
    protected Supplier<? extends InputStream> contents;
    protected String summary;
    protected String permissions;
    protected boolean allowFailure = false;
    protected boolean createDirectory = false;
    protected final ConfigBag config = ConfigBag.newInstance();

    protected WinRmPutTaskStub() {
    }

    protected WinRmPutTaskStub(WinRmPutTaskStub constructor) {
        this.remoteFile = constructor.remoteFile;
        this.machine = constructor.machine;
        this.contents = constructor.contents;
        this.summary = constructor.summary;
        this.allowFailure = constructor.allowFailure;
        this.createDirectory = constructor.createDirectory;
        this.permissions = constructor.permissions;
        this.config.copy(constructor.config);
    }

    public String getRemoteFile() {
        return remoteFile;
    }

    public String getSummary() {
        if (summary!=null) return summary;
        return "WinRm put: "+remoteFile;
    }

    public WinRmMachineLocation getMachine() {
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


    protected ConfigBag getConfig() {
        return config;
    }
}
