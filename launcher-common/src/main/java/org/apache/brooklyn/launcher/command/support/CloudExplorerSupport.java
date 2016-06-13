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
package org.apache.brooklyn.launcher.command.support;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationDefinition;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsUtil;
import org.apache.brooklyn.util.exceptions.FatalConfigurationRuntimeException;
import org.apache.brooklyn.util.stream.Streams;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.options.TemplateOptions;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Convenience for listing Cloud Compute and BlobStore details.
 * <p>
 * For fuller functionality, consider instead the jclouds CLI or Ruby Fog CLI.
 * <p>
 * The advantage of this utility is that it piggie-backs off the {@code brooklyn.property} credentials,
 * so requires less additional credential configuration. It also gives brooklyn-specific information,
 * such as which image will be used by default in a given cloud.
 */
public abstract class CloudExplorerSupport implements Callable<Void> {

    private ManagementContext managementContext;

    public static final String ALL_LOCATIONS_DESC =
        "All locations (i.e. all locations in brooklyn.properties for which there are credentials)";
    public boolean allLocations;

    public static final String LOCATION_DESC =
        "A location spec (e.g. referring to a named location in brooklyn.properties file)";
    public String location;

    public static final String AUTOCONFIRM_DESC = "Automatically answer yes to any questions";
    public boolean autoconfirm = false;


    @VisibleForTesting
    protected PrintStream stdout = System.out;

    @VisibleForTesting
    protected PrintStream stderr = System.err;

    @VisibleForTesting
    protected InputStream stdin = System.in;

    public CloudExplorerSupport(ManagementContext managementContext, boolean allLocations, String location, boolean autoconfirm) {
        this.managementContext = managementContext;
        this.allLocations = allLocations;
        this.location = location;
        this.autoconfirm = autoconfirm;
    }

    protected abstract void doCall(JcloudsLocation loc, String indent) throws Exception;

    @Override
    public Void call() throws Exception {
        List<JcloudsLocation> locs = Lists.newArrayList();
        if (location != null && allLocations) {
            throw new FatalConfigurationRuntimeException("Must not specify --location and --all-locations");
        } else if (location != null) {
            JcloudsLocation loc = (JcloudsLocation) managementContext.getLocationRegistry().getLocationManaged(location);
            locs.add(loc);
        } else if (allLocations) {
            // Find all named locations that point at different target clouds
            Map<String, LocationDefinition> definedLocations = managementContext.getLocationRegistry().getDefinedLocations();
            for (LocationDefinition locationDef : definedLocations.values()) {

                Location loc = managementContext.getLocationManager().createLocation(
                    managementContext.getLocationRegistry().getLocationSpec(locationDef).get() );

                if (loc instanceof JcloudsLocation) {
                    boolean found = false;
                    for (JcloudsLocation existing : locs) {
                        if (equalTargets(existing, (JcloudsLocation) loc)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        locs.add((JcloudsLocation) loc);
                    }
                }
            }
        } else {
            throw new FatalConfigurationRuntimeException("Must specify one of --location or --all-locations");
        }

        // TODO consider using org.apache.karaf.shell.support.table.ShellTable to replace/supplement this formatting,
        // here and elsewhere in this class
        for (JcloudsLocation loc : locs) {
            stdout.println("Location {");
            stdout.println("\tprovider: "+loc.getProvider());
            stdout.println("\tdisplayName: "+loc.getDisplayName());
            stdout.println("\tidentity: "+loc.getIdentity());
            if (loc.getEndpoint() != null) stdout.println("\tendpoint: "+loc.getEndpoint());
            if (loc.getRegion() != null) stdout.println("\tregion: "+loc.getRegion());

            try {
                doCall(loc, "\t");
            } finally {
                stdout.println("}");
            }
        }

        return null;
    }

    protected boolean equalTargets(JcloudsLocation loc1, JcloudsLocation loc2) {
        return Objects.equal(loc1.getProvider(), loc2.getProvider())
            && Objects.equal(loc1.getIdentity(), loc2.getIdentity())
            && Objects.equal(loc1.getEndpoint(), loc2.getEndpoint())
            && Objects.equal(loc1.getRegion(), loc2.getRegion());
    }


    public static abstract class ComputeExploration extends CloudExplorerSupport {
        protected abstract void doCall(ComputeService computeService, String indent) throws Exception;

        public ComputeExploration(ManagementContext managementContext, boolean allLocations, String location,
                                  boolean autoconfirm) {
            super(managementContext, allLocations, location, autoconfirm);
        }

        @Override
        protected void doCall(JcloudsLocation loc, String indent) throws Exception {
            ComputeService computeService = loc.getComputeService();
            doCall(computeService, indent);
        }
    }

    public static class ListInstances extends ComputeExploration {
        public static final String NAME = "list-instances";
        public static final String DESCRIPTION = "list instances";

        public ListInstances(ManagementContext managementContext, boolean allLocations, String location,
                             boolean autoconfirm) {
            super(managementContext, allLocations, location, autoconfirm);
        }

        @Override
        protected void doCall(ComputeService computeService, String indent) throws Exception {
            Set<? extends ComputeMetadata> instances = computeService.listNodes();
            stdout.println(indent+"Instances {");
            for (ComputeMetadata instance : instances) {
                stdout.println(indent+"\t"+instance);
            }
            stdout.println(indent+"}");
        }
    }

    public static class ListImages extends ComputeExploration {
        public static final String NAME = "list-images";
        public static final String DESCRIPTION = "list images";

        public ListImages(ManagementContext managementContext, boolean allLocations, String location,
                          boolean autoconfirm) {
            super(managementContext, allLocations, location, autoconfirm);
        }

        @Override
        protected void doCall(ComputeService computeService, String indent) throws Exception {
            Set<? extends Image> images = computeService.listImages();
            stdout.println(indent+"Images {");
            for (Image image : images) {
                stdout.println(indent+"\t"+image);
            }
            stdout.println(indent+"}");
        }
    }

    public static class ListHardwareProfiles extends ComputeExploration {
        public static final String NAME = "list-hardware-profiles";
        public static final String DESCRIPTION = "list hardware profiles";

        public ListHardwareProfiles(ManagementContext managementContext, boolean allLocations, String location,
                                    boolean autoconfirm) {
            super(managementContext, allLocations, location, autoconfirm);
        }

        @Override
        protected void doCall(ComputeService computeService, String indent) throws Exception {
            Set<? extends Hardware> hardware = computeService.listHardwareProfiles();
            stdout.println(indent+"Hardware Profiles {");
            for (Hardware image : hardware) {
                stdout.println(indent+"\t"+image);
            }
            stdout.println(indent+"}");
        }
    }

    public static class GetImage extends ComputeExploration {
        public static final String NAME = "get-image";
        public static final String DESCRIPTION = "get image details for one or more imageIds";
        public static final String ARGUMENT_NAME = "imageIds";
        public static final String ARGUMENT_DESC = "IDs of the images to retrieve";

        private List<String> names;

        public GetImage(ManagementContext managementContext, boolean allLocations, String location,
                        boolean autoconfirm, List<String> arguments) {
            super(managementContext, allLocations, location, autoconfirm);
            names = arguments;
        }

        @Override
        protected void doCall(ComputeService computeService, String indent) throws Exception {

            for (String imageId : names) {
                Image image = computeService.getImage(imageId);
                if (image == null) {
                    return;
                }
                stdout.println(indent+"Image "+imageId+" {");
                stdout.println(indent+"\t"+image);
                stdout.println(indent+"}");
            }
        }
    }

    public static class ComputeDefaultTemplate extends CloudExplorerSupport {
        public static final String NAME = "default-template";
        public static final String DESCRIPTION = "compute default template";

        public ComputeDefaultTemplate(ManagementContext managementContext, boolean allLocations, String location,
                                      boolean autoconfirm) {
            super(managementContext, allLocations, location, autoconfirm);
        }

        @Override
        protected void doCall(JcloudsLocation loc, String indent) throws Exception {

            ComputeService computeService = loc.getComputeService();

            Template template = loc.buildTemplate(computeService, loc.config().getBag());
            Image image = template.getImage();
            Hardware hardware = template.getHardware();
            org.jclouds.domain.Location location = template.getLocation();
            TemplateOptions options = template.getOptions();
            stdout.println(indent+"Default template {");
            stdout.println(indent+"\tImage: "+image);
            stdout.println(indent+"\tHardware: "+hardware);
            stdout.println(indent+"\tLocation: "+location);
            stdout.println(indent+"\tOptions: "+options);
            stdout.println(indent+"}");
        }
    }

    public static class TerminateInstances extends ComputeExploration {
        public static final String NAME = "terminate-instances";
        public static final String DESCRIPTION = "terminate instances for one or more instance IDs";
        public static final String ARGUMENT_NAME = "instanceIds";
        public static final String ARGUMENT_DESC = "IDs of the instances to terminate";
        private List<String> names;

        public TerminateInstances(ManagementContext managementContext, boolean allLocations, String location,
                                  boolean autoconfirm, List<String> names) {
            super(managementContext, allLocations, location, autoconfirm);
            this.names = names;
        }

        @Override
        protected void doCall(ComputeService computeService, String indent) throws Exception {

            for (String instanceId : names) {
                NodeMetadata instance = computeService.getNodeMetadata(instanceId);
                if (instance == null) {
                    stderr.println(indent+"Cannot terminate instance; could not find "+instanceId);
                } else {
                    boolean confirmed = confirm(indent, "terminate "+instanceId+" ("+instance+")");
                    if (confirmed) {
                        computeService.destroyNode(instanceId);
                    }
                }
            }
        }
    }

    public static abstract class Blobstore extends CloudExplorerSupport {
        public Blobstore(ManagementContext managementContext, boolean allLocations, String location,
                         boolean autoconfirm) {
            super(managementContext, allLocations, location, autoconfirm);
        }

        protected abstract void doCall(org.jclouds.blobstore.BlobStore blobstore, String indent) throws Exception;

        @Override
        protected void doCall(JcloudsLocation loc, String indent) throws Exception {
            String identity = checkNotNull(loc.getConfig(LocationConfigKeys.ACCESS_IDENTITY), "identity must not be null");
            String credential = checkNotNull(loc.getConfig(LocationConfigKeys.ACCESS_CREDENTIAL), "credential must not be null");
            String provider = checkNotNull(loc.getConfig(LocationConfigKeys.CLOUD_PROVIDER), "provider must not be null");
            String endpoint = loc.getConfig(CloudLocationConfig.CLOUD_ENDPOINT);

            BlobStoreContext context = JcloudsUtil.newBlobstoreContext(provider, endpoint, identity, credential);
            try {
                org.jclouds.blobstore.BlobStore blobStore = context.getBlobStore();
                doCall(blobStore, indent);
            } finally {
                context.close();
            }
        }
    }

    public static class BlobstoreListContainers extends Blobstore {
        public static final String NAME = "list-containers";
        public static final String DESCRIPTION = "list containers";

        public BlobstoreListContainers(ManagementContext managementContext, boolean allLocations, String location,
                                       boolean autoconfirm) {
            super(managementContext, allLocations, location, autoconfirm);
        }

        @Override
        protected void doCall(BlobStore blobstore, String indent) throws Exception {
            Set<? extends StorageMetadata> containers = blobstore.list();
            stdout.println(indent+"Containers {");
            for (StorageMetadata container : containers) {
                stdout.println(indent+"\t"+container);
            }
            stdout.println(indent+"}");
        }
    }

    public static class BlobstoreListContainer extends Blobstore {
        public static final String NAME = "list-container";
        public static final String DESCRIPTION = "list container details for one or more container names";
        public static final String ARGUMENT_NAME = "container-names";
        public static final String ARGUMENT_DESC = "names of the containers to list";
        private List<String> names;

        public BlobstoreListContainer(ManagementContext managementContext, boolean allLocations, String location,
                                      boolean autoconfirm, List<String> names) {
            super(managementContext, allLocations, location, autoconfirm);
            this.names = names;
        }

        @Override
        protected void doCall(BlobStore blobStore, String indent) throws Exception {
            for (String containerName : names) {
                Set<? extends StorageMetadata> contents = blobStore.list(containerName);
                stdout.println(indent+"Container "+containerName+" {");
                for (StorageMetadata content : contents) {
                    stdout.println(indent+"\t"+content);
                }
                stdout.println(indent+"}");
            }
        }
    }

    public static class GetBlob extends Blobstore {
        public static final String NAME = "blob";
        public static final String DESCRIPTION = "list blob details for a given container and blob";
        public static final String CONTAINER_ARGUMENT_NAME = "--container";
        public static final String CONTAINER_ARGUMENT_DESC = "name of the container of the blob";
        public static final String BLOB_ARGUMENT_NAME = "--blob";
        public static final String BLOB_ARGUMENT_DESC = "name of the blob in the container";
        public String container;
        public String blob;

        public GetBlob(ManagementContext managementContext, boolean allLocations, String location, boolean autoconfirm,
                       String container, String blob) {
            super(managementContext, allLocations, location, autoconfirm);
            this.container = container;
            this.blob = blob;
        }

        @Override
        protected void doCall(BlobStore blobStore, String indent) throws Exception {
            Blob content = blobStore.getBlob(container, blob);
            stdout.println(indent+"Blob "+container+" : " +blob +" {");
            stdout.println(indent+"\tHeaders {");
            for (Map.Entry<String, String> entry : content.getAllHeaders().entries()) {
                stdout.println(indent+"\t\t"+entry.getKey() + " = " + entry.getValue());
            }
            stdout.println(indent+"\t}");
            stdout.println(indent+"\tmetadata : "+content.getMetadata());
            stdout.println(indent+"\tpayload : "+ Streams.readFullyStringAndClose(content.getPayload().openStream()));
            stdout.println(indent+"}");
        }
    }

    protected boolean confirm(String msg, String indent) throws Exception {
        if (autoconfirm) {
            stdout.println(indent+"Auto-confirmed: "+msg);
            return true;
        } else {
            stdout.println(indent+"Enter y/n. Are you sure you want to "+msg);
            int in = stdin.read();
            boolean confirmed = (Character.toLowerCase(in) == 'y');
            if (confirmed) {
                stdout.println(indent+"Confirmed; will "+msg);
            } else {
                stdout.println(indent+"Declined; will not "+msg);
            }
            return confirmed;
        }
    }
}
