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
package org.apache.brooklyn.karaf.commands.cloud.explorer;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;

public abstract class AbstractCloudExplorerCommand implements Action {

    public static final String CLOUD_EXPLORER_SCOPE = "cloud";

    @Reference
    private ManagementContext managementContext;

    @Option(name = "--all-locations", description = "all locations")
    protected boolean allLocations;

    @Option(name = "--location", aliases = "-l", description = "a location specification")
    protected String location;

    @Option(name = "--auto-confirm", aliases = { "-y", "--yes" }, description = "automatically accept confirmation prompts")
    protected boolean autoConfirm;

    public void setManagementContext(ManagementContext managementContext) {
        this.managementContext = managementContext;
    }

    public ManagementContext getManagementContext() {
        return managementContext;
    }

    public void unsetManagementContext() {
        this.managementContext = null;
    }
}
