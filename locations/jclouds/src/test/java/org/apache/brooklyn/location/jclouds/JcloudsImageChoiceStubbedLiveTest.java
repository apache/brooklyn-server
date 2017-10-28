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

import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.BasicNodeCreator;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.NodeCreator;
import org.apache.brooklyn.util.collections.MutableMap;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class JcloudsImageChoiceStubbedLiveTest extends AbstractJcloudsStubbedLiveTest {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(JcloudsImageChoiceStubbedLiveTest.class);
    
    private Template template;
    
    @Override
    protected NodeCreator newNodeCreator() {
        return new BasicNodeCreator() {
            @Override protected NodeMetadata newNode(String group, Template template) {
                JcloudsImageChoiceStubbedLiveTest.this.template = template;
                return super.newNode(group, template);
            }
        };
    }

    @Override
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
    
    @Test(groups={"Live", "Live-sanity"})
    @SuppressWarnings("deprecation")
    public void testJcloudsCreateWithNulls() throws Exception {
        obtainMachine(MutableMap.builder()
                .put(JcloudsLocationConfig.IMAGE_ID, "DEBIAN_8_64")
                .put(JcloudsLocationConfig.MIN_RAM, null)
                .put(JcloudsLocationConfig.MIN_CORES, null)
                .put(JcloudsLocationConfig.MIN_DISK, null)
                .put(JcloudsLocationConfig.HARDWARE_ID, null)
                .put(JcloudsLocationConfig.OS_64_BIT, null)
                .put(JcloudsLocationConfig.OS_FAMILY, null)
                .put(JcloudsLocationConfig.IMAGE_DESCRIPTION_REGEX, null)
                .put(JcloudsLocationConfig.IMAGE_NAME_REGEX, null)
                .put(JcloudsLocationConfig.OS_VERSION_REGEX, null)
                .put(JcloudsLocationConfig.SECURITY_GROUPS, null)
                .put(JcloudsLocationConfig.INBOUND_PORTS, null)
                .put(JcloudsLocationConfig.USER_METADATA_STRING, null)
                .put(JcloudsLocationConfig.STRING_TAGS, null)
                .put(JcloudsLocationConfig.USER_METADATA_MAP, null)
                .put(JcloudsLocationConfig.EXTRA_PUBLIC_KEY_DATA_TO_AUTH, null)
                .put(JcloudsLocationConfig.RUN_AS_ROOT, null)
                .put(JcloudsLocationConfig.LOGIN_USER, null)
                .put(JcloudsLocationConfig.LOGIN_USER_PASSWORD, null)
                .put(JcloudsLocationConfig.LOGIN_USER_PRIVATE_KEY_FILE, null)
                .put(JcloudsLocationConfig.KEY_PAIR, null)
                .put(JcloudsLocationConfig.AUTO_GENERATE_KEYPAIRS, null)
                .put(JcloudsLocationConfig.AUTO_ASSIGN_FLOATING_IP, null)
                .put(JcloudsLocationConfig.NETWORK_NAME, null)
                .put(JcloudsLocationConfig.DOMAIN_NAME, null)
                .put(JcloudsLocationConfig.TEMPLATE_OPTIONS, null)
                .build());
        assertEquals(template.getImage().getId(), "DEBIAN_8_64", "template="+template);
    }
    
    @Test(groups={"Live", "Live-sanity"})
    public void testJcloudsCreateWithNullTemplateOptionVal() throws Exception {
        obtainMachine(MutableMap.builder()
                .put(JcloudsLocationConfig.IMAGE_ID, "DEBIAN_8_64")
                .put(JcloudsLocationConfig.TEMPLATE_OPTIONS, MutableMap.of("domainName", null))
                .build());
        assertEquals(template.getImage().getId(), "DEBIAN_8_64", "template="+template);
    }
}
