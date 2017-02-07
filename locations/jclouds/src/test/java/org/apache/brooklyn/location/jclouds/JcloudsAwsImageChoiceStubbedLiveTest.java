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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.BasicNodeCreator;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.NodeCreator;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class JcloudsAwsImageChoiceStubbedLiveTest extends AbstractJcloudsStubbedLiveTest {

    // TODO These tests are most likely quite brittle. If a newer version of the AMIs are released 
    // then the image details might change. However, better having these tests than not testing it
    // at all in my opinion. If they prove brittle, we can relax the assertions (e.g. that it is
    // version "7.*" rather than exactly "7.0").

    private static final Logger LOG = LoggerFactory.getLogger(JcloudsAwsImageChoiceStubbedLiveTest.class);
    
    private static final String LOCATION_SPEC = AWS_EC2_PROVIDER + ":" + AWS_EC2_USEAST_REGION_NAME;
    
    private Template template;
    
    @Override
    protected String getLocationSpec() {
        return LOCATION_SPEC;
    }

    @Override
    protected NodeCreator newNodeCreator() {
        return new BasicNodeCreator() {
            @Override protected NodeMetadata newNode(String group, Template template) {
                JcloudsAwsImageChoiceStubbedLiveTest.this.template = template;
                return super.newNode(group, template);
            }
        };
    }

    @Test(groups={"Live", "Live-sanity"})
    public void testSpecificImageId() throws Exception {
        // Amazon's "SUSE Linux Enterprise Server 12 SP2 (HVM), SSD Volume Type"
        String imageId = AWS_EC2_USEAST_REGION_NAME + "/ami-6f86a478";
        
        obtainMachine(ImmutableMap.of(JcloudsLocation.IMAGE_ID, imageId));
        Image image = template.getImage();

        assertEquals(image.getId(), imageId, "image="+image);
    }
    
    // See testUbuntu14Image
    @Test(groups={"Live", "Live-sanity"})
    public void testDefault() throws Exception {
        obtainMachine(ImmutableMap.of());
        Image image = template.getImage();

        LOG.info("default="+image);
        assertCentos(image, "7.0");
    }
    
    // See testUbuntu14Image
    @Test(groups={"Live", "Live-sanity"})
    public void testUbuntu() throws Exception {
        obtainMachine(ImmutableMap.of(JcloudsLocation.OS_FAMILY, "ubuntu"));
        Image image = template.getImage();

        LOG.info("ubuntu="+image);
        assertUbuntu(image, "14.04");
    }

    // {id=us-east-1/ami-a05194cd, providerId=ami-a05194cd, name=ubuntu/images/ebs-ssd/ubuntu-xenial-16.04-amd64-server-20160610, location={scope=REGION, id=us-east-1, description=us-east-1, parent=aws-ec2, iso3166Codes=[US-VA]}, os={family=ubuntu, arch=paravirtual, version=, description=099720109477/ubuntu/images/ebs-ssd/ubuntu-xenial-16.04-amd64-server-20160610, is64Bit=true}, description=099720109477/ubuntu/images/ebs-ssd/ubuntu-xenial-16.04-amd64-server-20160610, version=20160610, status=AVAILABLE[available], loginUser=ubuntu, userMetadata={owner=099720109477, rootDeviceType=ebs, virtualizationType=paravirtual, hypervisor=xen}}
    @Test(groups={"Live", "Live-sanity"})
    public void testUbuntu14() throws Exception {
        obtainMachine(ImmutableMap.of(JcloudsLocation.OS_FAMILY, "ubuntu", JcloudsLocation.OS_VERSION_REGEX, "14"));
        Image image = template.getImage();

        LOG.info("ubuntu_14="+image);
        assertUbuntu(image, "14.04");
    }
    
    // TODO See https://issues.apache.org/jira/browse/BROOKLYN-400
    @Test(groups={"Broken", "Live", "Live-sanity"})
    public void testUbuntu16() throws Exception {
        obtainMachine(ImmutableMap.of(JcloudsLocation.OS_FAMILY, "ubuntu", JcloudsLocation.OS_VERSION_REGEX, "16.*"));
        Image image = template.getImage();

        LOG.info("ubuntu_16="+image);
        assertUbuntu(image, "16.04");
    }
    
    // See testCentos7
    @Test(groups={"Live", "Live-sanity"})
    public void testCentos() throws Exception {
        obtainMachine(ImmutableMap.of(JcloudsLocation.OS_FAMILY, "centos"));
        Image image = template.getImage();
        
        LOG.info("centos="+image);
        assertCentos(image, "7.0");
        
    }
    
    // {id=us-east-1/ami-5492ba3c, providerId=ami-5492ba3c, name=RightImage_CentOS_7.0_x64_v14.2.1_HVM_EBS, location={scope=REGION, id=us-east-1, description=us-east-1, parent=aws-ec2, iso3166Codes=[US-VA]}, os={family=centos, arch=hvm, version=7.0, description=411009282317/RightImage_CentOS_7.0_x64_v14.2.1_HVM_EBS, is64Bit=true}, description=RightImage_CentOS_7.0_x64_v14.2.1_HVM_EBS, version=14.2.1_HVM_EBS, status=AVAILABLE[available], loginUser=root, userMetadata={owner=411009282317, rootDeviceType=ebs, virtualizationType=hvm, hypervisor=xen}}
    @Test(groups={"Live", "Live-sanity"})
    public void testCentos7() throws Exception {
        Thread.sleep(10*1000);
        obtainMachine(ImmutableMap.of(JcloudsLocation.OS_FAMILY, "centos", JcloudsLocation.OS_VERSION_REGEX, "7"));
        Image image = template.getImage();
        
        LOG.info("centos_7="+image);
        assertCentos(image, "7.0");
    }
    
    // {id=us-east-1/ami-d89fb7b0, providerId=ami-d89fb7b0, name=RightImage_CentOS_6.6_x64_v14.2.1_HVM_EBS, location={scope=REGION, id=us-east-1, description=us-east-1, parent=aws-ec2, iso3166Codes=[US-VA]}, os={family=centos, arch=hvm, version=6.6, description=411009282317/RightImage_CentOS_6.6_x64_v14.2.1_HVM_EBS, is64Bit=true}, description=RightImage_CentOS_6.6_x64_v14.2.1_HVM_EBS, version=14.2.1_HVM_EBS, status=AVAILABLE[available], loginUser=root, userMetadata={owner=411009282317, rootDeviceType=ebs, virtualizationType=hvm, hypervisor=xen}}
    @Test(groups={"Live", "Live-sanity"})
    public void testCentos6() throws Exception {
        obtainMachine(ImmutableMap.of(JcloudsLocation.OS_FAMILY, "centos", JcloudsLocation.OS_VERSION_REGEX, "6"));
        Image image = template.getImage();
        
        LOG.info("centos_6="+image);
        assertCentos(image, "6.6");
    }
    
    @Test(groups={"Live", "Live-sanity"})
    public void testCentos7ImageInSingapore() throws Exception {
        replaceJcloudsLocation(AWS_EC2_PROVIDER + ":" + AWS_EC2_SINGAPORE_REGION_NAME);
        
        obtainMachine(ImmutableMap.of(JcloudsLocation.OS_FAMILY, "centos", JcloudsLocation.OS_VERSION_REGEX, "7"));
        Image image = template.getImage();
        
        LOG.info("centos_7="+image);
        assertCentos(image, "7.0");
    }
    
    private void assertUbuntu(Image image, String version) {
        // Expect owner Canonical 
        assertEquals(image.getUserMetadata().get("owner"), "099720109477", "image="+image);
        assertTrue(image.getName().toLowerCase().contains("ubuntu"), "image="+image);
        assertTrue(image.getName().contains(version), "image="+image);
        assertEquals(image.getOperatingSystem().getFamily(), OsFamily.UBUNTU, "image="+image);
        assertEquals(image.getOperatingSystem().getVersion(), version, "image="+image);
    }
    
    private void assertCentos(Image image, String expectedVersion) {
        // Expect owner RightScale (i.e. "RightImage")
        assertEquals(image.getUserMetadata().get("owner"), "411009282317", "image="+image);
        assertTrue(image.getName().toLowerCase().contains("centos"), "image="+image);
        assertTrue(image.getName().contains(expectedVersion), "image="+image);
        assertEquals(image.getOperatingSystem().getFamily(), OsFamily.CENTOS, "image="+image);
        assertEquals(image.getOperatingSystem().getVersion(), expectedVersion, "image="+image);
    }
}
