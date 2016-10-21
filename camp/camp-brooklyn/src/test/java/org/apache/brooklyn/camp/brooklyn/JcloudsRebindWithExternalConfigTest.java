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

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.camp.brooklyn.AbstractJcloudsStubYamlTest.ByonComputeServiceStaticRef;
import org.apache.brooklyn.camp.brooklyn.spi.creation.CampTypePlanTransformer;
import org.apache.brooklyn.core.config.external.InPlaceExternalConfigSupplier;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.location.jclouds.ComputeServiceRegistry;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

/**
 * It changes the setup for the test in the following ways:
 * <ul>
 *   <li>Brooklyn properties defines external config
 * </ul>
 */
@Test
public class JcloudsRebindWithExternalConfigTest extends AbstractJcloudsRebindStubYamlTest {

    @Override
    protected BrooklynProperties createBrooklynProperties() {
        BrooklynProperties result = super.createBrooklynProperties();
        result.put("brooklyn.external.creds", InPlaceExternalConfigSupplier.class.getName());
        result.put("brooklyn.external.creds.test-identity", "myidentity");
        result.put("brooklyn.external.creds.test-credential", "mycredential");
        
        return result;
    }
    
    @Override
    protected JcloudsLocation newJcloudsLocation(ComputeServiceRegistry computeServiceRegistry) throws Exception {
        ByonComputeServiceStaticRef.setInstance(computeServiceRegistry);
        
        String yaml = Joiner.on("\n").join(
                "location:",
                "  "+LOCATION_CATALOG_ID+":",
                "    identity: $brooklyn:external(\"creds\", \"test-identity\")",
                "    credential: $brooklyn:external(\"creds\", \"test-credential\")",
                "services:\n"+
                "- type: org.apache.brooklyn.entity.stock.BasicApplication");
        
        EntitySpec<?> spec = 
                mgmt().getTypeRegistry().createSpecFromPlan(CampTypePlanTransformer.FORMAT, yaml, RegisteredTypeLoadingContexts.spec(Application.class), EntitySpec.class);
        final Entity app = mgmt().getEntityManager().createEntity(spec);
        app.invoke(Startable.START, ImmutableMap.<String, Object>of()).get();

        JcloudsLocation result = (JcloudsLocation) Iterables.getOnlyElement(app.getLocations());
        assertEquals(result.getIdentity(), "myidentity");
        assertEquals(result.getCredential(), "mycredential");
        
        return result;
    }
}
