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

import java.util.Map;

import org.apache.brooklyn.util.collections.MutableMap;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadata.Status;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.compute.domain.Template;
import org.jclouds.domain.LoginCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class JcloudsImageChoiceStubbedLiveTest extends AbstractJcloudsStubbedLiveTest {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(JcloudsImageChoiceStubbedLiveTest.class);
    
    private Template template;
    
    @Override
    protected NodeCreator newNodeCreator() {
        return new NodeCreator() {
            @Override protected NodeMetadata newNode(String group, Template template) {
                JcloudsImageChoiceStubbedLiveTest.this.template = template;
                
                NodeMetadata result = new NodeMetadataBuilder()
                        .id("myid")
                        .credentials(LoginCredentials.builder().identity("myuser").credential("mypassword").build())
                        .loginPort(22)
                        .status(Status.RUNNING)
                        .publicAddresses(ImmutableList.of("173.194.32.123"))
                        .privateAddresses(ImmutableList.of("172.168.10.11"))
                        .build();
                return result;
            }
        };
    }

    protected Map<Object, Object> jcloudsLocationConfig(Map<Object, Object> defaults) {
        return ImmutableMap.<Object, Object>builder()
                .putAll(defaults)
                .put(JcloudsLocationConfig.IMAGE_ID, "CENTOS_5_64")
                .build();
    }

    @Test(groups={"Live", "Live-sanity"})
    public void testJcloudsCreateWithImageId() throws Exception {
        obtainMachine(ImmutableMap.of(JcloudsLocationConfig.IMAGE_ID, "DEBIAN_8_64"));
        assertEquals(template.getImage().getId(), "DEBIAN_8_64", "template="+template);
    }
    
    @Test(groups={"Live", "Live-sanity"})
    public void testJcloudsCreateWithImageDescriptionRegex() throws Exception {
        // overrides the imageId specified in jcloudsLocationConfig
        obtainMachine(MutableMap.of(JcloudsLocationConfig.IMAGE_DESCRIPTION_REGEX, ".*DEBIAN_8_64.*", JcloudsLocationConfig.IMAGE_ID, null));
        assertEquals(template.getImage().getId(), "DEBIAN_8_64", "template="+template);
    }
}
