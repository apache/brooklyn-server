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

import org.apache.brooklyn.launcher.command.support.CloudExplorerSupport.GetBlob;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Service;

import static org.apache.brooklyn.karaf.commands.cloud.explorer.AbstractCloudExplorerCommand.CLOUD_EXPLORER_SCOPE;

@Command(scope = CLOUD_EXPLORER_SCOPE, name = GetBlob.NAME, description = GetBlob.DESCRIPTION)
@Service
public class BlobCommand extends AbstractCloudExplorerCommand {

    @Option(name = GetBlob.CONTAINER_ARGUMENT_NAME, description = GetBlob.CONTAINER_ARGUMENT_DESC, required = true)
    private String container;

    @Option(name = GetBlob.BLOB_ARGUMENT_NAME, description = GetBlob.BLOB_ARGUMENT_DESC, required = true)
    private String blob;

    @Override
    public Object execute() throws Exception {

        final GetBlob getBlob =
            new GetBlob(getManagementContext(), allLocations, location, autoConfirm, container, blob);
        return getBlob.call();
    }
}
