/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.entity.software.base.test.location;

import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.core.mgmt.BrooklynTags.NamedStringTag;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsLocationTest.FakeLocalhostWithParentJcloudsLocation;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class SoftwareProcessLocationUnmanageTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testLocalhostLocationUnmanagedOnStop() {
        LocationSpec<LocalhostMachineProvisioningLocation> locationSpec = LocationSpec.create(TestApplication.LOCALHOST_PROVISIONER_SPEC);
        testLocationUnmanagedOnStop(locationSpec);
    }

    @Test(groups="Live")
    public void testDockerLocationUnmanagedOnStop() {
        LocationSpec<? extends Location> locationSpec = LocationSpec.create(JcloudsLocation.class)
                .configure(JcloudsLocation.CLOUD_PROVIDER, "docker")
                .configure(JcloudsLocation.CLOUD_ENDPOINT, "http://<docker-ip>:2375")
                // Either the certificates for a tls enabled endpoint or any valid keys for non-tls endpoints (still required by jclouds)
                .configure(JcloudsLocation.ACCESS_IDENTITY, "<path to>/cert.pem")
                .configure(JcloudsLocation.ACCESS_CREDENTIAL, "<path to>/key.pem")
                // needs "docker pull ubuntu:14.04" beforehand
                .configure(JcloudsLocation.IMAGE_DESCRIPTION_REGEX, "ubuntu:14.04")
                .configure(JcloudsLocation.WAIT_FOR_SSHABLE, "false");
        testLocationUnmanagedOnStop(locationSpec);
    }

    @Test
    public void testJcloudsLocationUnmanagedOnStop() {
        LocationSpec<? extends Location> locationSpec = LocationSpec.create(FakeLocalhostWithParentJcloudsLocation.class)
                .configure(JcloudsLocation.CLOUD_PROVIDER, "aws-ec2")
                .configure(JcloudsLocation.ACCESS_IDENTITY, "dummy")
                .configure(JcloudsLocation.ACCESS_CREDENTIAL, "xxx")
                .configure(JcloudsLocation.WAIT_FOR_SSHABLE, "false");
        testLocationUnmanagedOnStop(locationSpec);
    }

    private void testLocationUnmanagedOnStop(LocationSpec<? extends Location> locationSpec) {
        EntitySpec<BasicApplication> appSpec = EntitySpec.create(BasicApplication.class)
            .location(locationSpec)
            .child(EntitySpec.create(EmptySoftwareProcess.class)
                    .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, Boolean.TRUE)
                    .configure(EmptySoftwareProcess.USE_SSH_MONITORING, Boolean.FALSE));

        BasicApplication app = mgmt.getEntityManager().createEntity(appSpec);
        Entity child = Iterables.getOnlyElement(app.getChildren());

        Assert.assertEquals(child.getLocations(), ImmutableList.of());

        app.start(ImmutableList.<Location>of());
        Location appLocation = Iterables.getOnlyElement(app.getLocations());
        assertOwned(app, appLocation);
        Location entityLocation = Iterables.getOnlyElement(child.getLocations());
        // No owner tag because it's created by SoftwareProcess, not by Brooklyn internals
        assertNotOwned(entityLocation);
        app.stop();

        Assert.assertEquals(child.getLocations(), ImmutableList.of());

        Assert.assertFalse(mgmt.getEntityManager().isManaged(child));
        Assert.assertFalse(mgmt.getEntityManager().isManaged(app));
        Set<Location> locs = ImmutableSet.copyOf(mgmt.getLocationManager().getLocations());
        Assert.assertFalse(locs.contains(entityLocation), locs + " should not contain " + entityLocation);
        Assert.assertFalse(locs.contains(appLocation), locs + " should not contain " + appLocation);
    }

    private void assertOwned(BasicApplication app, Location loc) {
        NamedStringTag ownerEntityTag = BrooklynTags.findFirst(BrooklynTags.OWNER_ENTITY_ID, loc.tags().getTags());
        Assert.assertNotNull(ownerEntityTag);
        Assert.assertEquals(ownerEntityTag.getContents(), app.getId());
    }

    private void assertNotOwned(Location loc) {
        NamedStringTag ownerEntityTag = BrooklynTags.findFirst(BrooklynTags.OWNER_ENTITY_ID, loc.tags().getTags());
        Assert.assertNull(ownerEntityTag);
    }

}
