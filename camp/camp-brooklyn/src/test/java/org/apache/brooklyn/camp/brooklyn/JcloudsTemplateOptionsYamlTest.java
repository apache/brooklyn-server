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

import java.util.NoSuchElementException;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.camp.brooklyn.JcloudsCustomizerInstantiationYamlDslTest.RecordingLocationCustomizer;
import org.apache.brooklyn.camp.brooklyn.spi.creation.CampTypePlanTransformer;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.entity.machine.MachineEntity;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.googlecomputeengine.compute.options.GoogleComputeEngineTemplateOptions;
import org.jclouds.googlecomputeengine.domain.Instance.ServiceAccount;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

/**
 * Tests that jcouds TemplateOptions are constructed properly from yaml blueprints.
 */
@Test
public class JcloudsTemplateOptionsYamlTest extends AbstractJcloudsStubYamlTest {

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
        return "jclouds:google-compute-engine:us-central1-a";
    }
    
    @Test
    public void testCoercesStronglyTypedTemplateOption() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location: " + LOCATION_CATALOG_ID,
                "services:\n" +
                "- type: " + MachineEntity.class.getName(),
                "  brooklyn.config:",
                "    onbox.base.dir.skipResolution: true",
                "    sshMonitoring.enabled: false",
                "    metrics.usage.retrieve: false",
                "    enabled: true",
                "    provisioning.properties:",
                "      templateOptions:",
                "        serviceAccounts:",
                "        - email: myemail",
                "          scopes:",
                "          - myscope1",
                "          - myscope2",
                "      customizer:",
                "        $brooklyn:object:",
                "          type: " + RecordingLocationCustomizer.class.getName(),
                "          object.fields:",
                "            enabled: $brooklyn:config(\"enabled\")");

        EntitySpec<?> spec = managementContext.getTypeRegistry().createSpecFromPlan(CampTypePlanTransformer.FORMAT, yaml, RegisteredTypeLoadingContexts.spec(Application.class), EntitySpec.class);
        Entity app = managementContext.getEntityManager().createEntity(spec);

        app.invoke(Startable.START, ImmutableMap.<String, Object>of()).get();

        GoogleComputeEngineTemplateOptions options = (GoogleComputeEngineTemplateOptions) findTemplateOptionsInCustomizerArgs();
        assertEquals(options.serviceAccounts(), ImmutableList.of(
                ServiceAccount.create("myemail", ImmutableList.of("myscope1", "myscope2"))));
    }
    
    private TemplateOptions findTemplateOptionsInCustomizerArgs() {
        for (RecordingLocationCustomizer.CallParams call : RecordingLocationCustomizer.calls) {
            Optional<?> templateOptions = Iterables.tryFind(call.args, Predicates.instanceOf(TemplateOptions.class));
            if (templateOptions.isPresent()) {
                return (TemplateOptions) templateOptions.get();
            }
        }
        throw new NoSuchElementException();
    }
}
