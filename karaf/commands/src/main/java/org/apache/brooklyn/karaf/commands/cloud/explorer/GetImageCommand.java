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

import org.apache.brooklyn.launcher.command.support.CloudExplorerSupport.GetImage;
import org.apache.karaf.shell.api.action.Argument;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Service;

import java.util.ArrayList;
import java.util.List;

import static org.apache.brooklyn.karaf.commands.cloud.explorer.AbstractCloudExplorerCommand.CLOUD_EXPLORER_SCOPE;

@Command(scope = CLOUD_EXPLORER_SCOPE, name = GetImage.NAME, description = GetImage.DESCRIPTION)
@Service
public class GetImageCommand extends AbstractCloudExplorerCommand {

    @Argument(name = GetImage.ARGUMENT_NAME, description = GetImage.ARGUMENT_DESC, required = true, multiValued = true)
    private List<String> imageIds = new ArrayList<>();

    @Override
    public Object execute() throws Exception {

        final GetImage getImage = new GetImage(getManagementContext(), allLocations, location, autoConfirm, imageIds);
        return getImage.call();
    }
}
