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

import org.apache.brooklyn.location.jclouds.BasicJcloudsLocationCustomizer;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsSshMachineLocation;
import org.jclouds.aws.ec2.compute.AWSEC2ComputeService;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.TemplateBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

public abstract class AbstractEbsVolumeCustomizer extends BasicJcloudsLocationCustomizer {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractEbsVolumeCustomizer.class);

    protected String availabilityZone;
    protected String ec2DeviceName;
    protected String osDeviceName;
    protected String mountPoint;
    protected String owner;
    protected Integer permissions;

    public void setAvailabilityZone(String availabilityZone) {
        this.availabilityZone = availabilityZone;
    }

    public void setEc2DeviceName(String ec2DeviceName) {
        this.ec2DeviceName = ec2DeviceName;
    }

    public void setOsDeviceName(String osDeviceName) {
        this.osDeviceName = osDeviceName;
    }

    public void setMountPoint(String mountPoint) {
        this.mountPoint = mountPoint;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public void setPermissions(Integer permissions) {
        this.permissions = permissions;
    }

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, TemplateBuilder templateBuilder) {
        if (computeService instanceof AWSEC2ComputeService) {
            templateBuilder.locationId(availabilityZone);
        } else {
            LOG.debug("Skipping configuration of non-EC2 ComputeService {}", computeService);
        }
    }

    protected void createFilesystem(JcloudsSshMachineLocation machine, String filesystemType) {
        machine.execCommands("Creating filesystem on EBS volume", ImmutableList.of(
                "sudo mkfs." + filesystemType + " " + osDeviceName
        ));
    }

    protected void mountFilesystem(JcloudsSshMachineLocation machine) {
        // NOTE: also adds an entry to fstab so the mount remains available after a reboot.
        Builder<String> commands = ImmutableList.<String>builder().add(
                "sudo mkdir -m 000 " + mountPoint,
                "sudo echo \"" + osDeviceName + " " + mountPoint + " auto noatime 0 0\" | sudo tee -a /etc/fstab",
                "sudo mount " + mountPoint,
                "sudo chown " + (owner == null ? machine.getUser() : owner) + " " + mountPoint
        );
        if (permissions != null) {
            commands.add("sudo chmod " + permissions + " " + mountPoint);
        }
        machine.execCommands("Mounting EBS volume", commands.build());
    }
}