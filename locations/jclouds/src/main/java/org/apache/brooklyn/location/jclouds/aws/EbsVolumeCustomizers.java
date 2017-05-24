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
package org.apache.brooklyn.location.jclouds.aws;

import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsMachineLocation;
import org.apache.brooklyn.location.jclouds.JcloudsSshMachineLocation;
import org.jclouds.aws.ec2.AWSEC2Api;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.ec2.compute.EC2ComputeService;
import org.jclouds.ec2.compute.options.EC2TemplateOptions;
import org.jclouds.ec2.features.ElasticBlockStoreApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Customization hooks to ensure that any EC2 instances provisioned via a corresponding jclouds location become associated
 * with an EBS volume (either an existing volume, specified by ID, or newly created).
 */
public class EbsVolumeCustomizers {

    private static final Logger LOG = LoggerFactory.getLogger(EbsVolumeCustomizers.class);

    /**
     * Location customizer that:
     * <ul>
     * <li>configures the AWS availability zone</li>
     * <li>creates a new EBS volume of the requested size in the given availability zone</li>
     * <li>attaches the new volume to the newly-provisioned EC2 instance</li>
     * <li>formats the new volume with the requested filesystem</li>
     * <li>mounts the filesystem under the requested path</li>
     * </ul>
     */
    public static class WithNewVolume extends AbstractEbsVolumeCustomizer {
        private String filesystemType;
        private int sizeInGib;
        private boolean deleteOnTermination;

        public void setFilesystemType(String filesystemType) {
            this.filesystemType = filesystemType;
        }

        public void setSizeInGib(int sizeInGib) {
            this.sizeInGib = sizeInGib;
        }

        public void setDeleteOnTermination(boolean deleteOnTermination) {
            this.deleteOnTermination = deleteOnTermination;
        }

        @Override
        public void customize(JcloudsLocation location, ComputeService computeService, TemplateOptions templateOptions) {
            if (templateOptions instanceof EC2TemplateOptions) {
                ((EC2TemplateOptions) templateOptions).mapNewVolumeToDeviceName(ec2DeviceName, sizeInGib, deleteOnTermination);
            } else {
                LOG.debug("Skipping configuration of non-EC2 TemplateOptions {}", templateOptions);
            }
        }

        @Override
        public void customize(JcloudsLocation location, ComputeService computeService, JcloudsMachineLocation machine) {
            if (computeService instanceof EC2ComputeService) {
                createFilesystem((JcloudsSshMachineLocation) machine, filesystemType);
                mountFilesystem((JcloudsSshMachineLocation) machine);
            } else {
                LOG.debug("Skipping configuration of non-EC2 ComputeService {}", computeService);
            }
        }
    }

    /**
     * Location customizer that:
     * <ul>
     * <li>configures the AWS availability zone</li>
     * <li>obtains a new EBS volume from the specified snapshot in the given availability zone</li>
     * <li>attaches the new volume to the newly-provisioned EC2 instance</li>
     * <li>mounts the filesystem under the requested path</li>
     * </ul>
     */
    public static class WithExistingSnapshot extends AbstractEbsVolumeCustomizer {
        private String snapshotId;
        private int sizeInGib;
        private boolean deleteOnTermination;

        public void setSnapshotId(String snapshotId) {
            this.snapshotId = snapshotId;
        }

        public void setSizeInGib(int sizeInGib) {
            this.sizeInGib = sizeInGib;
        }

        public void setDeleteOnTermination(boolean deleteOnTermination) {
            this.deleteOnTermination = deleteOnTermination;
        }

        @Override
        public void customize(JcloudsLocation location, ComputeService computeService, TemplateOptions templateOptions) {
            if (templateOptions instanceof EC2TemplateOptions) {
                ((EC2TemplateOptions) templateOptions).mapEBSSnapshotToDeviceName(ec2DeviceName, snapshotId, sizeInGib, deleteOnTermination);
            } else {
                LOG.debug("Skipping configuration of non-EC2 TemplateOptions {}", templateOptions);
            }
        }

        @Override
        public void customize(JcloudsLocation location, ComputeService computeService, JcloudsMachineLocation machine) {
            if (computeService instanceof EC2ComputeService) {
                mountFilesystem((JcloudsSshMachineLocation) machine);
            } else {
                LOG.debug("Skipping configuration of non-EC2 ComputeService {}", computeService);
            }
        }
    }

    /**
     * Location customizer that:
     * <ul>
     * <li>configures the AWS availability zone</li>
     * <li>attaches the specified (existing) volume to the newly-provisioned EC2 instance</li>
     * <li>mounts the filesystem under the requested path</li>
     * </ul>
     */
    public static class WithExistingVolume extends AbstractEbsVolumeCustomizer {
        private String region;
        private String volumeId;

        public void setRegion(String region) {
            this.region = region;
        }

        public void setVolumeId(String volumeId) {
            this.volumeId = volumeId;
        }

        @Override
        public void customize(JcloudsLocation location, ComputeService computeService, JcloudsMachineLocation machine) {
            if (computeService instanceof EC2ComputeService) {
                AWSEC2Api ec2Client = computeService.getContext().unwrapApi(AWSEC2Api.class);
                ElasticBlockStoreApi ebsClient = ec2Client.getElasticBlockStoreApi().get();
                ebsClient.attachVolumeInRegion(region, volumeId, machine.getJcloudsId(), ec2DeviceName);
                mountFilesystem((JcloudsSshMachineLocation) machine);
            } else {
                LOG.debug("Skipping configuration of non-EC2 ComputeService {}", computeService);
            }
        }
    }

    // Prevent construction: helper class.
    private EbsVolumeCustomizers() {
    }

}