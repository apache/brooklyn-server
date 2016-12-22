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

import java.util.NoSuchElementException;

import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.BasicNodeCreator;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.NodeCreator;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class JcloudsGceHardwareProfilesStubbedLiveTest extends AbstractJcloudsStubbedLiveTest {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(JcloudsGceHardwareProfilesStubbedLiveTest.class);
    
    private static final String LOCATION_SPEC = GCE_PROVIDER + ":" + GCE_USCENTRAL_REGION_NAME;
    
    private static final String N1_STANDARD_1_HARDWARE_ID = "n1-standard-1";
    private static final String G1_SMALL_HARDWARE_ID = "g1-small";
    private static final String N1_STANDARD_2_HARDWARE_ID = "n1-standard-2";
    private static final String N1_HIGHCPU_4_HARDWARE_ID = "n1-highcpu-4";
    
    private static final String GCE_ZONES_PREFIX = "https://www.googleapis.com/compute/v1/projects/jclouds-gce/zones/";
    private static final String G1_SMALL_HARDWARE_ID_LONG_FORM = GCE_ZONES_PREFIX + GCE_USCENTRAL_REGION_NAME + "/machineTypes/" + G1_SMALL_HARDWARE_ID;
    private static final String N1_STANDARD_1_HARDWARE_ID_LONG_FORM = GCE_ZONES_PREFIX + GCE_USCENTRAL_REGION_NAME + "/machineTypes/" + N1_STANDARD_1_HARDWARE_ID;
    private static final String N1_STANDARD_2_HARDWARE_ID_LONG_FORM = GCE_ZONES_PREFIX + GCE_USCENTRAL_REGION_NAME + "/machineTypes/" + N1_STANDARD_2_HARDWARE_ID;
    private static final String N1_HIGHCPU_4_HARDWARE_ID_LONG_FORM = GCE_ZONES_PREFIX + GCE_USCENTRAL_REGION_NAME + "/machineTypes/" + N1_HIGHCPU_4_HARDWARE_ID;
    
    private Template template;

    @Override
    protected String getLocationSpec() {
        return LOCATION_SPEC;
    }

    @Override
    protected NodeCreator newNodeCreator() {
        return new BasicNodeCreator() {
            @Override protected NodeMetadata newNode(String group, Template template) {
                JcloudsGceHardwareProfilesStubbedLiveTest.this.template = template;
                return super.newNode(group, template);
            }
        };
    }

    @Test(groups={"Live", "Live-sanity"})
    public void testJcloudsCreateWithHardwareProfiles() throws Exception {
        // default minRam is 1gb
        obtainMachine();
        assertTrue(template.getHardware().getRam() >= 1000, "template="+template);
        assertEquals(template.getHardware().getId(), G1_SMALL_HARDWARE_ID_LONG_FORM, "template="+template);
        
        obtainMachine(MutableMap.of(JcloudsLocationConfig.MIN_RAM, "4096"));
        assertTrue(template.getHardware().getRam() >= 4096, "template="+template);
        assertEquals(template.getHardware().getId(), N1_STANDARD_2_HARDWARE_ID_LONG_FORM, "template="+template);
        
        obtainMachine(MutableMap.of(JcloudsLocationConfig.MIN_CORES, "4"));
        assertTrue(template.getHardware().getProcessors().get(0).getCores() >= 4, "template="+template);
        assertEquals(template.getHardware().getId(), N1_HIGHCPU_4_HARDWARE_ID_LONG_FORM, "template="+template);
    }

    @Test(groups={"Live", "Live-sanity"})
    public void testJcloudsCreateWithHardwareIdLongForm() throws Exception {
        obtainMachine(ImmutableMap.of(JcloudsLocation.HARDWARE_ID, N1_STANDARD_1_HARDWARE_ID_LONG_FORM));
        
        assertEquals(template.getHardware().getId(), N1_STANDARD_1_HARDWARE_ID_LONG_FORM, "template="+template);
    }
    
    @Test(groups={"Live", "Live-sanity"})
    public void testJcloudsCreateWithHardwareIdLongFormWithNoRegionInLocationSpec() throws Exception {
        // region will be passed in the obtain() call
        replaceJcloudsLocation(GCE_PROVIDER);
        
        obtainMachine(ImmutableMap.of(
                JcloudsLocation.CLOUD_REGION_ID, GCE_USCENTRAL_REGION_NAME, 
                JcloudsLocation.HARDWARE_ID, N1_STANDARD_1_HARDWARE_ID_LONG_FORM));
        
        assertEquals(template.getHardware().getId(), N1_STANDARD_1_HARDWARE_ID_LONG_FORM, "template="+template);
    }
    
    @Test(groups={"Live", "Live-sanity"})
    public void testJcloudsCreateWithHardwareIdLongFormWithNoRegion() throws Exception {
        // Expect it to default to GCE_USCENTRAL_REGION_NAME - 
        // but is that only because that region is hard-coded in the long hardwareId?!
        replaceJcloudsLocation(GCE_PROVIDER);
        
        obtainMachine(ImmutableMap.of(JcloudsLocation.HARDWARE_ID, N1_STANDARD_1_HARDWARE_ID_LONG_FORM));
        
        assertEquals(template.getHardware().getId(), N1_STANDARD_1_HARDWARE_ID_LONG_FORM, "template="+template);
    }
    
    // See https://issues.apache.org/jira/browse/JCLOUDS-1108
    @Test(groups={"Live", "Live-sanity"})
    public void testJcloudsCreateWithHardwareIdShortForm() throws Exception {
        obtainMachine(ImmutableMap.of(JcloudsLocation.HARDWARE_ID, N1_STANDARD_1_HARDWARE_ID));
        
        assertEquals(template.getHardware().getId(), N1_STANDARD_1_HARDWARE_ID_LONG_FORM, "template="+template);
    }
    
    // See https://issues.apache.org/jira/browse/JCLOUDS-1108
    @Test(groups={"Live", "Live-sanity"})
    public void testJcloudsCreateWithHardwareIdShortFormWithNoRegionInLocationSpec() throws Exception {
        replaceJcloudsLocation(GCE_PROVIDER);
        
        obtainMachine(ImmutableMap.of(
                JcloudsLocation.CLOUD_REGION_ID, GCE_USCENTRAL_REGION_NAME, 
                JcloudsLocation.HARDWARE_ID, N1_STANDARD_1_HARDWARE_ID));
        
        assertEquals(template.getHardware().getId(), N1_STANDARD_1_HARDWARE_ID_LONG_FORM, "template="+template);
    }
    
    // See https://issues.apache.org/jira/browse/JCLOUDS-1108
    // Region needs to be specified somewhere.
    @Test(groups={"Live", "Live-sanity"})
    public void testJcloudsCreateWithHardwareIdShortFormWithNoRegion() throws Exception {
        replaceJcloudsLocation(GCE_PROVIDER);
        
        try {
            obtainMachine(ImmutableMap.of(JcloudsLocation.HARDWARE_ID, N1_STANDARD_1_HARDWARE_ID));
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            NoSuchElementException nsee = Exceptions.getFirstThrowableOfType(e, NoSuchElementException.class);
            if (nsee == null || !nsee.toString().contains("hardwareId("+N1_STANDARD_1_HARDWARE_ID+") not found")) {
                throw e;
            }
        }
    }
}
