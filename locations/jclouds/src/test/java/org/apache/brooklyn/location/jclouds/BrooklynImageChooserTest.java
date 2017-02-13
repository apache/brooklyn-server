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
package org.apache.brooklyn.location.jclouds;

import java.net.URI;
import java.util.Map;
import java.util.Set;

import org.jclouds.compute.domain.ComputeType;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.OperatingSystem;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.domain.Location;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.domain.ResourceMetadata;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;

public class BrooklynImageChooserTest {

    private BrooklynImageChooser brooklynImageChooser;

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        brooklynImageChooser = new BrooklynImageChooser();
    }

    @Test
    public void testCentosOverUbuntu() {
        assertOrderOfPreference(
                getScore(OsFamily.CENTOS, "7.0"),
                getScore(OsFamily.UBUNTU, "14.04"));
    }

    @Test
    public void testCentos7then6then5() {
        assertOrderOfPreference(
                getScore(OsFamily.CENTOS, "7.0"),
                getScore(OsFamily.CENTOS, "6.6"),
                getScore(OsFamily.CENTOS, "5.4"));
    }

    @Test
    public void testUbuntu14then12then11() {
        assertOrderOfPreference(
                getScore(OsFamily.UBUNTU, "14.04"),
                getScore(OsFamily.UBUNTU, "12.04"),
                getScore(OsFamily.UBUNTU, "11.04"));
    }

    @Test
    public void testCentosUbuntuRHEL() {
        assertOrderOfPreference(
                getScore(OsFamily.CENTOS, "7.0"),
                getScore(OsFamily.UBUNTU, "14.04"),
                getScore(OsFamily.RHEL, "7.0"));
    }

    double getScore(OsFamily centos, String version) {
        return brooklynImageChooser.score(getImg(centos, version));
    }

    void assertOrderOfPreference(Double... scores) {
        Assert.assertTrue(Ordering.natural().reverse().isStrictlyOrdered(ImmutableList.copyOf(scores)), "Images not ordered in correct preference " + Joiner.on(",").join(scores));
    }

    Image getImg(final OsFamily osFamily, final String version) {
        return new Image() {
            @Override
            public OperatingSystem getOperatingSystem() {
                return new OperatingSystem(osFamily, "", version, "", "", true);
            }

            @Override
            public String getVersion() {
                return version;
            }

            @Override
            public String getDescription() {
                return null;
            }

            @Override
            public LoginCredentials getDefaultCredentials() {
                return null;
            }

            @Override
            public Status getStatus() {
                return null;
            }

            @Override
            public String getBackendStatus() {
                return null;
            }

            @Override
            public ComputeType getType() {
                return null;
            }

            @Override
            public String getProviderId() {
                return null;
            }

            @Override
            public String getName() {
                return null;
            }

            @Override
            public String getId() {
                return null;
            }

            @Override
            public Set<String> getTags() {
                return null;
            }

            @Override
            public Location getLocation() {
                return null;
            }

            @Override
            public URI getUri() {
                return null;
            }

            @Override
            public Map<String, String> getUserMetadata() {
                return ImmutableMap.of();
            }

            @Override
            public int compareTo(ResourceMetadata<ComputeType> o) {
                return 0;
            }
        };
    }
}