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

import static org.apache.brooklyn.camp.brooklyn.JcloudsCustomizerInstantiationYamlDslTest
    .RecordingLocationCustomizer.findTemplateOptionsInCustomizerArgs;
import static org.apache.brooklyn.test.LogWatcher.EventPredicates.containsMessage;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.camp.brooklyn.JcloudsCustomizerInstantiationYamlDslTest.RecordingLocationCustomizer;
import org.apache.brooklyn.camp.brooklyn.spi.creation.CampTypePlanTransformer;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.entity.machine.MachineEntity;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.location.jclouds.templates.customize.TemplateOptionsOption;
import org.apache.brooklyn.test.LogWatcher;
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Tests that jcouds TemplateOptions are constructed properly from yaml blueprints.
 */
@Test
public class JcloudsTemplateOptionsYamlAwsTest extends AbstractJcloudsStubYamlTest {

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        RecordingLocationCustomizer.clear();
        super.setUp();
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            RecordingLocationCustomizer.clear();
        }
    }
    
    @Override
    protected String getLocationSpec() {
        return "jclouds:aws-ec2:us-east-1";
    }

    @Test
    public void testDslCanBeUsedInTemplateOptions() throws Exception {

        String subnetValue = "subnet-123456";

        String yaml = Joiner.on("\n").join(
            "location: " + LOCATION_CATALOG_ID,
            "services:",
            "  - type: " + BasicEntity.class.getName(),
            "    id: ent1",
            "    brooklyn.config:",
            "      subnet.value: " + subnetValue,
            "  - type: " + MachineEntity.class.getName(),
            "    brooklyn.config:",
            "      onbox.base.dir.skipResolution: true",
            "      sshMonitoring.enabled: false",
            "      metrics.usage.retrieve: false",
            "      enabled: true",
            "      provisioning.properties:",
            "        templateOptions:",
            "          subnetId: $brooklyn:entity(\"ent1\").config(\"subnet.value\")",
            "        customizer:",
            "          $brooklyn:object:",
            "            type: " + RecordingLocationCustomizer.class.getName(),
            "            object.fields:",
            "              enabled: $brooklyn:config(\"enabled\")");

        final String ignoreOption = "Ignoring request to set template option";

        try (final LogWatcher watcher = new LogWatcher(
                ImmutableList.of(LoggerFactory.getLogger(TemplateOptionsOption.class).getName()),
                ch.qos.logback.classic.Level.WARN,
                Predicates.and(containsMessage(ignoreOption), containsMessage("subnetId")))) {
            EntitySpec<?> spec = managementContext.getTypeRegistry().createSpecFromPlan(
                CampTypePlanTransformer.FORMAT, yaml,
                RegisteredTypeLoadingContexts.spec(Application.class), EntitySpec.class);
            Entity app = managementContext.getEntityManager().createEntity(spec);

            app.invoke(Startable.START, ImmutableMap.<String, Object>of()).get();

            assertTrue(watcher.getEvents().isEmpty(), ignoreOption);

            AWSEC2TemplateOptions options = (AWSEC2TemplateOptions) findTemplateOptionsInCustomizerArgs();
            assertEquals(options.getSubnetId(), subnetValue);
        }

    }
}
