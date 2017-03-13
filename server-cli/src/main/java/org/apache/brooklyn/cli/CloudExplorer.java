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
package org.apache.brooklyn.cli;

import org.apache.brooklyn.launcher.command.support.CloudExplorerSupport;
import org.apache.brooklyn.launcher.command.support.CloudExplorerSupport.BlobstoreListContainer;
import org.apache.brooklyn.launcher.command.support.CloudExplorerSupport.BlobstoreListContainers;
import org.apache.brooklyn.launcher.command.support.CloudExplorerSupport.GetBlob;
import org.apache.brooklyn.launcher.command.support.CloudExplorerSupport.GetImage;
import org.apache.brooklyn.launcher.command.support.CloudExplorerSupport.ComputeDefaultTemplate;
import org.apache.brooklyn.launcher.command.support.CloudExplorerSupport.ListImages;
import org.apache.brooklyn.launcher.command.support.CloudExplorerSupport.ListInstances;
import org.apache.brooklyn.launcher.command.support.CloudExplorerSupport.ListHardwareProfiles;
import org.apache.brooklyn.launcher.command.support.CloudExplorerSupport.TerminateInstances;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.ParseException;

import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;

import com.google.common.base.Objects.ToStringHelper;

/**
 * Makes use of {@link CloudExplorerSupport} to provide cloud explorer commands at Brooklyn server command line.
 */
public class CloudExplorer {

    public static abstract class JcloudsCommand extends AbstractMain.BrooklynCommandCollectingArgs {
        @Option(name = { "--all-locations" }, title = "all locations",
                description = CloudExplorerSupport.ALL_LOCATIONS_DESC)
        public boolean allLocations;
        
        @Option(name = { "-l", "--location" }, title = "location spec",
                description = CloudExplorerSupport.LOCATION_DESC)
        public String location;

        @Option(name = { "-y", "--yes" }, title = "auto-confirm",
                description = CloudExplorerSupport.AUTOCONFIRM_DESC)
        public boolean autoConfirm = false;


        protected abstract CloudExplorerSupport getExplorer(LocalManagementContext mgmt, boolean allLocations,
                                                            String location, boolean autoconfirm);

        @Override
        public Void call() throws Exception {
            LocalManagementContext mgmt = new LocalManagementContext();
            try {
                CloudExplorerSupport explorer = getExplorer(mgmt, allLocations, location, autoConfirm);
                explorer.call();
            } finally {
                mgmt.terminate();
            }
            return null;
        }

        @Override
        public ToStringHelper string() {
            return super.string()
                    .add("location", location);
        }
    }
    

    @Command(name = ListInstances.NAME, description = ListInstances.DESCRIPTION)
    public static class ComputeListInstancesCommand extends JcloudsCommand {
        @Override
        protected CloudExplorerSupport
        getExplorer(LocalManagementContext mgmt, boolean allLocations, String location, boolean autoconfirm) {
            failIfArguments();
            return new ListInstances(mgmt, allLocations, location, autoconfirm);
        }
    }

    @Command(name = ListImages.NAME, description = ListImages.DESCRIPTION)
    public static class ComputeListImagesCommand extends JcloudsCommand {
        @Override
        protected CloudExplorerSupport
        getExplorer(LocalManagementContext mgmt, boolean allLocations, String location, boolean autoconfirm) {
            failIfArguments();
            return new ListImages(mgmt, allLocations, location, autoconfirm);
        }
    }
    
    @Command(name = ListHardwareProfiles.NAME, description = ListHardwareProfiles.DESCRIPTION)
    public static class ComputeListHardwareProfilesCommand extends JcloudsCommand {
        @Override
        protected CloudExplorerSupport
        getExplorer(LocalManagementContext mgmt, boolean allLocations, String location, boolean autoconfirm) {
            failIfArguments();
            return new CloudExplorerSupport.ListHardwareProfiles(mgmt, allLocations, location, autoconfirm);
        }
    }
    
    @Command(name = GetImage.NAME, description = GetImage.DESCRIPTION)
    public static class ComputeGetImageCommand extends JcloudsCommand {

        @Override
        protected CloudExplorerSupport
        getExplorer(LocalManagementContext mgmt, boolean allLocations, String location, boolean autoconfirm) {
            if (arguments.isEmpty()) {
                throw new ParseException("Requires at least one image-id arguments");
            }
            return new GetImage(mgmt, allLocations, location, autoconfirm, arguments);
        }

        @Override
        public ToStringHelper string() {
            return super.string()
                    .add(GetImage.ARGUMENT_NAME, arguments);
        }
    }

    @Command(name = ComputeDefaultTemplate.NAME, description = ComputeDefaultTemplate.DESCRIPTION)
    public static class ComputeDefaultTemplateCommand extends JcloudsCommand {

        @Override
        protected CloudExplorerSupport
        getExplorer(LocalManagementContext mgmt, boolean allLocations, String location, boolean autoconfirm) {
            failIfArguments();
            return new CloudExplorerSupport.ComputeDefaultTemplate(mgmt, allLocations, location, autoconfirm);
        }
    }
    
    @Command(name = TerminateInstances.NAME, description = TerminateInstances.DESCRIPTION)
    public static class ComputeTerminateInstancesCommand extends JcloudsCommand {

        @Override
        protected CloudExplorerSupport
        getExplorer(LocalManagementContext mgmt, boolean allLocations, String location, boolean autoconfirm) {
            if (arguments.isEmpty()) {
                throw new ParseException("Requires at least one instance-id arguments");
            }
            return new CloudExplorerSupport.TerminateInstances(mgmt, allLocations, location, autoconfirm, arguments);
        }

        @Override
        public ToStringHelper string() {
            return super.string()
                    .add(TerminateInstances.ARGUMENT_NAME, arguments);
        }
    }


    @Command(name = BlobstoreListContainers.NAME, description = BlobstoreListContainers.DESCRIPTION)
    public static class BlobstoreListContainersCommand extends JcloudsCommand {

        @Override
        protected CloudExplorerSupport
        getExplorer(LocalManagementContext mgmt, boolean allLocations, String location, boolean autoconfirm) {
            failIfArguments();
            return new BlobstoreListContainers(mgmt, allLocations, location, autoconfirm);
        }
    }

    @Command(name = BlobstoreListContainer.NAME, description = BlobstoreListContainer.DESCRIPTION)
    public static class BlobstoreListContainerCommand extends JcloudsCommand {

        @Override
        protected CloudExplorerSupport
        getExplorer(LocalManagementContext mgmt, boolean allLocations, String location, boolean autoconfirm) {
            if (arguments.isEmpty()) {
                throw new ParseException("Requires at least one container-name arguments");
            }
            return new BlobstoreListContainer(mgmt, allLocations, location, autoconfirm, arguments);
        }

        @Override
        public ToStringHelper string() {
            return super.string()
                    .add(BlobstoreListContainer.ARGUMENT_NAME, arguments);
        }
    }
    
    @Command(name = GetBlob.NAME, description = GetBlob.DESCRIPTION)
    public static class BlobstoreGetBlobCommand extends JcloudsCommand {
        @Option(name = { GetBlob.CONTAINER_ARGUMENT_NAME}, description = GetBlob.CONTAINER_ARGUMENT_DESC)
        public String container;

        @Option(name = { GetBlob.BLOB_ARGUMENT_NAME}, description = GetBlob.BLOB_ARGUMENT_DESC)
        public String blob;

        @Override
        protected CloudExplorerSupport
        getExplorer(LocalManagementContext mgmt, boolean allLocations, String location, boolean autoconfirm) {
            failIfArguments();
            return new GetBlob(mgmt, allLocations, location, autoconfirm, container, blob);
        }

        @Override
        public ToStringHelper string() {
            return super.string()
                    .add("container", container)
                    .add("blob", blob);
        }
    }
}
