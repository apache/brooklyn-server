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
package org.apache.brooklyn.karaf.commands;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.apache.karaf.shell.support.table.ShellTable;

@Command(scope = "brooklyn", name = "application-list", description = "Lists all applications")
@Service
public class ApplicationList implements Action {

    @Reference
    private ManagementContext managementContext;

    @Override
    public Object execute() throws Exception {
        System.out.println();
        ShellTable table = new ShellTable();
        table.column("id");
        table.column("name");
        table.column("up");
        table.column("state");
        table.column("expected state");

        managementContext.getApplications().forEach((application -> table.addRow().addContent(
                application.getApplicationId(),
                application.getDisplayName(),
                application.sensors().get(Attributes.SERVICE_UP).toString(),
                application.sensors().get(Attributes.SERVICE_STATE_ACTUAL).toString(),
                application.sensors().get(Attributes.SERVICE_STATE_EXPECTED).toString()
        )));
        table.print(System.out, true);
        System.out.println();
        return null;
    }
}
