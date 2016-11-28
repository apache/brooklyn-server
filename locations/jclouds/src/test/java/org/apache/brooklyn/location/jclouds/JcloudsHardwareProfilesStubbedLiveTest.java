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
import org.apache.brooklyn.util.collections.MutableMap;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class JcloudsHardwareProfilesStubbedLiveTest extends AbstractJcloudsStubbedLiveTest {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(JcloudsHardwareProfilesStubbedLiveTest.class);
    
    private Template template;
    
    @Override
    protected NodeCreator newNodeCreator() {
        return new BasicNodeCreator() {
            @Override protected NodeMetadata newNode(String group, Template template) {
                JcloudsHardwareProfilesStubbedLiveTest.this.template = template;
                return super.newNode(group, template);
            }
        };
    }

    @Test(groups={"Live", "Live-sanity"})
    public void testJcloudsCreateWithHardwareProfiles() throws Exception {
        // default minRam is 1gb (but default smallest VM in softlayer is 1024mb so not a particularly useful test!)
        obtainMachine();
        assertTrue(template.getHardware().getRam() >= 1000, "template="+template);
        
        obtainMachine(MutableMap.of(JcloudsLocationConfig.MIN_RAM, "4096"));
        assertTrue(template.getHardware().getRam() >= 4096, "template="+template);
        
        obtainMachine(MutableMap.of(JcloudsLocationConfig.MIN_CORES, "4"));
        assertTrue(template.getHardware().getProcessors().get(0).getCores() >= 4, "template="+template);

        obtainMachine(MutableMap.of(JcloudsLocationConfig.MIN_DISK, "51"));
        assertTrue(template.getHardware().getVolumes().get(0).getSize() >= 51, "template="+template);
        
        String hardwareId = "cpu=1,memory=6144,disk=25,type=LOCAL";
        obtainMachine(MutableMap.of(JcloudsLocationConfig.HARDWARE_ID, hardwareId));
        assertEquals(template.getHardware().getId(), hardwareId, "template="+template);
    }
    
}
