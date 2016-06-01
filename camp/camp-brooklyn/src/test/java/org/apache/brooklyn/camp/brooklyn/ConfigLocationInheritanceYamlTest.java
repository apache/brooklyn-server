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
package org.apache.brooklyn.camp.brooklyn;

import static org.testng.Assert.assertEquals;

import java.io.StringReader;
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationRegistry;
import org.apache.brooklyn.api.location.LocationResolver;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.location.BasicLocationRegistry;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.location.jclouds.ComputeServiceRegistry;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsLocationConfig;
import org.apache.brooklyn.location.jclouds.JcloudsLocationResolver;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.SingleNodeCreator;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadata.Status;
import org.jclouds.compute.domain.OperatingSystem;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.Processor;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.Volume;
import org.jclouds.compute.domain.internal.HardwareImpl;
import org.jclouds.compute.domain.internal.NodeMetadataImpl;
import org.jclouds.domain.LocationScope;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.domain.internal.LocationImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Requires credentials for jclouds:aws-ec2 in the default brooklyn.properties.
 */
public class ConfigLocationInheritanceYamlTest extends AbstractYamlTest {
    
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(ConfigLocationInheritanceYamlTest.class);

    private static final String CLOUD_PROVIDER = "aws-ec2";
    private static final String CLOUD_REGION = "us-east-1";
    private static final String CLOUD_IMAGE_ID = "us-east-1/ami-a96b01c0";
    
    private LocationImpl locImpl = new LocationImpl(
            LocationScope.REGION, 
            "myLocId", 
            "myLocDescription", 
            null, 
            ImmutableList.<String>of(), // iso3166Codes 
            ImmutableMap.<String,Object>of()); // metadata

    private NodeMetadata node = new NodeMetadataImpl(
            CLOUD_PROVIDER, 
            "myname", 
            "123", // ids in SoftLayer are numeric
            locImpl,
            URI.create("http://myuri.com"), 
            ImmutableMap.<String, String>of(), // userMetadata 
            ImmutableSet.<String>of(), // tags
            "mygroup",
            new HardwareImpl(
                    "myHardwareProviderId", 
                    "myHardwareName", 
                    "myHardwareId", 
                    locImpl, 
                    URI.create("http://myuri.com"), 
                    ImmutableMap.<String, String>of(), // userMetadata 
                    ImmutableSet.<String>of(), // tags
                    ImmutableList.<Processor>of(), 
                    1024, 
                    ImmutableList.<Volume>of(), 
                    Predicates.<Image>alwaysTrue(), // supportsImage, 
                    (String)null, // hypervisor
                    false),
            CLOUD_IMAGE_ID,
            new OperatingSystem(
                    OsFamily.CENTOS, 
                    "myOsName", 
                    "myOsVersion", 
                    "myOsArch", 
                    "myDescription", 
                    true), // is64Bit
            Status.RUNNING,
            "myBackendStatus",
            22, // login-port
            ImmutableList.of("1.2.3.4"), // publicAddresses, 
            ImmutableList.of("10.2.3.4"), // privateAddresses, 
            LoginCredentials.builder().identity("myidentity").password("mypassword").build(), 
            "myHostname");

    private SingleNodeCreator nodeCreator;

    @Override
    protected boolean useDefaultProperties() {
        return true;
    }

    public static class RecordingJcloudsLocation extends JcloudsLocation {
        public final List<ConfigBag> templateConfigs = Lists.newCopyOnWriteArrayList();
        
        public Template buildTemplate(ComputeService computeService, ConfigBag config) {
            templateConfigs.add(config);
            return super.buildTemplate(computeService, config);
        }
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        nodeCreator = new SingleNodeCreator(node);
        final ComputeServiceRegistry computeServiceRegistry = new StubbedComputeServiceRegistry(nodeCreator);

        LocationResolver resolver = new JcloudsLocationResolver() {
            @Override
            public String getPrefix() {
                return "jclouds-config-test";
            }

            protected Class<? extends JcloudsLocation> getLocationClass() {
                return RecordingJcloudsLocation.class;
            }

            @Override
            public LocationSpec<?> newLocationSpecFromString(String spec, Map<?,?> locationFlags, LocationRegistry registry) {
                LocationSpec<? extends Location> orig = super.newLocationSpecFromString(spec, locationFlags, registry);
                return LocationSpec.create(orig)
                        .configure(JcloudsLocationConfig.IMAGE_ID, CLOUD_IMAGE_ID)
                        .configure(JcloudsLocationConfig.COMPUTE_SERVICE_REGISTRY, computeServiceRegistry)
                        .configure(JcloudsLocationConfig.USE_JCLOUDS_SSH_INIT, false)
                        .configure(JcloudsLocationConfig.WAIT_FOR_SSHABLE, "false")
                        .configure(JcloudsLocationConfig.LOOKUP_AWS_HOSTNAME, false)
                        .configure("sshToolClass", RecordingSshTool.class.getName());
            }
        };

        ((BasicLocationRegistry)mgmt().getLocationRegistry()).registerResolver(resolver);

        addCatalogItems(
                "brooklyn.catalog:",
                "  id: jclouds-config-test-with-conf",
                "  name: stubbed-jclouds-gce",
                "  itemType: location",
                "  item:",
                "    type: jclouds-config-test:"+CLOUD_PROVIDER+":"+CLOUD_REGION,
                "    brooklyn.config:",
                "      minRam: 1234",
                "      templateOptions:",
                "        networks:",
                "        - mynetwork");
    }
    
    @Test(groups="Live")
    public void testUsesLocationProperties() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location: jclouds-config-test-with-conf",
                "services:",
                "- type: org.apache.brooklyn.entity.software.base.EmptySoftwareProcess");
        
        Entity app = createStartWaitAndLogApplication(new StringReader(yaml));
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        
        assertMachineConfig(
                Machines.findUniqueMachineLocation(entity.getLocations()).get(),
                ImmutableMap.of(JcloudsLocationConfig.MIN_RAM, 1234),
                ImmutableMap.of("networks", ImmutableList.of("mynetwork")));
    }
    
    @Test(groups="Live")
    public void testMergesLocationProperties() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location: jclouds-config-test-with-conf",
                "services:",
                "- type: org.apache.brooklyn.entity.software.base.EmptySoftwareProcess",
                "  brooklyn.config:",
                "    provisioning.properties:",
                "      minCores: 2",
                "      templateOptions:",
                "        subnetId: mysubnet");
        
        Entity app = createStartWaitAndLogApplication(new StringReader(yaml));
        Entity entity = Iterables.getOnlyElement(app.getChildren());

        assertMachineConfig(
                Machines.findUniqueMachineLocation(entity.getLocations()).get(),
                ImmutableMap.of(JcloudsLocationConfig.MIN_RAM, 1234, JcloudsLocationConfig.MIN_CORES, 2),
                ImmutableMap.of("networks", ImmutableList.of("mynetwork"), "subnetId", "mysubnet"));
    }
    
    @Test(groups="Live")
    public void testMergesCatalogEntityLocationProperties() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: EmptySoftwareProcess-with-conf",
                "  itemType: entity",
                "  item:",
                "    type: org.apache.brooklyn.entity.software.base.EmptySoftwareProcess",
                "    brooklyn.config:",
                "      provisioning.properties:",
                "        minDisk: 10g",
                "        templateOptions:",
                "          placementGroup: myPlacementGroup");

        String yaml = Joiner.on("\n").join(
                "location: jclouds-config-test-with-conf",
                "services:",
                "- type: EmptySoftwareProcess-with-conf",
                "  brooklyn.config:",
                "    provisioning.properties:",
                "      minCores: 2",
                "      templateOptions:",
                "        subnetId: mysubnet");

        Entity app = createStartWaitAndLogApplication(new StringReader(yaml));
        Entity entity = Iterables.getOnlyElement(app.getChildren());

        assertMachineConfig(
                Machines.findUniqueMachineLocation(entity.getLocations()).get(),
                ImmutableMap.of(JcloudsLocationConfig.MIN_RAM, 1234, JcloudsLocationConfig.MIN_CORES, 2, JcloudsLocationConfig.MIN_DISK, "10g"),
                ImmutableMap.of("networks", ImmutableList.of("mynetwork"), "subnetId", "mysubnet", "placementGroup", "myPlacementGroup"));
    }

    // TODO This doesn't work yet. Unfortunately the YAML parsing for entity and location items
    // is different (e.g. BrooklynComponentTemplateResolver.decorateSpec only deals with entities).
    // That is too big to deal with in this pull request that targets entity config!
    @Test(groups="Live", enabled=false)
    public void testMergesCatalogLocationProperties() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: extending-jclouds-config-test-with-conf",
                "  name: jclouds-config-test-with-conf",
                "  itemType: location",
                "  item:",
                "    type: jclouds-config-test:"+CLOUD_PROVIDER+":"+CLOUD_REGION,
                "    brooklyn.config:",
                "      minCores: 2",
                "      templateOptions:",
                "        subnetId: mysubnet");

        String yaml = Joiner.on("\n").join(
                "location: extending-jclouds-config-test-with-conf",
                "services:",
                "- type: org.apache.brooklyn.entity.software.base.EmptySoftwareProcess");

        Entity app = createStartWaitAndLogApplication(new StringReader(yaml));
        Entity entity = Iterables.getOnlyElement(app.getChildren());

        assertMachineConfig(
                Machines.findUniqueMachineLocation(entity.getLocations()).get(),
                ImmutableMap.of(JcloudsLocationConfig.MIN_RAM, 1234, JcloudsLocationConfig.MIN_CORES, 2),
                ImmutableMap.of("networks", ImmutableList.of("mynetwork"), "subnetId", "mysubnet"));
    }

    protected void assertMachineConfig(MachineLocation machine, Map<? extends ConfigKey<?>, ?> expectedTopLevel, Map<String, ?> expectedTemplateOptions) {
        RecordingJcloudsLocation jcloudsLocation = (RecordingJcloudsLocation) machine.getParent();
        ConfigBag conf = jcloudsLocation.templateConfigs.get(jcloudsLocation.templateConfigs.size()-1);
        
        Map<ConfigKey<?>, Object> subConf = Maps.newLinkedHashMap();
        for (Map.Entry<? extends ConfigKey<?>, ?> entry : expectedTopLevel.entrySet()) {
            subConf.put(entry.getKey(), conf.get(entry.getKey()));
        }
        
        assertEquals(subConf, expectedTopLevel, "actual="+subConf);
        
        Map<String, Object> actualTemplateOptions = conf.get(JcloudsLocationConfig.TEMPLATE_OPTIONS);
        for (Map.Entry<String, ?> entry : expectedTemplateOptions.entrySet()) {
            assertEquals(actualTemplateOptions.get(entry.getKey()), entry.getValue(), "templateOptions="+actualTemplateOptions);
        }
    }
}
