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

import java.io.StringReader;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.camp.brooklyn.ExternalConfigYamlTest.MyExternalConfigSupplier;
import org.apache.brooklyn.camp.brooklyn.ExternalConfigYamlTest.MyExternalConfigSupplierWithoutMapArg;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.StartableApplication;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.guava.Maybe;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class LocationExternalConfigYamlTest extends AbstractYamlRebindTest {

    private static final ConfigKey<String> MY_CONFIG_KEY = ConfigKeys.newStringConfigKey("my.config.key");
    
    @Override
    protected BrooklynProperties createBrooklynProperties() {
        BrooklynProperties props = super.createBrooklynProperties();
        props.put("brooklyn.external.myprovider", MyExternalConfigSupplier.class.getName());
        props.put("brooklyn.external.myprovider.mykey", "myval");
        props.put("brooklyn.external.myproviderWithoutMapArg", MyExternalConfigSupplierWithoutMapArg.class.getName());
        return props;
    }

    @Test(groups="Integration")
    public void testLocalhostInheritance() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: "+EmptySoftwareProcess.class.getName(),
                "location:",
                "  localhost:",
                "    my.config.key: $brooklyn:external(\"myprovider\", \"mykey\")");

        origApp = (StartableApplication) createAndStartApplication(new StringReader(yaml));

        Entity entity = Iterables.getOnlyElement( origApp.getChildren() );
        Location l = Iterables.getOnlyElement( entity.getLocations() );
        assertEquals(l.config().get(MY_CONFIG_KEY), "myval");
                
        Maybe<Object> rawConfig = ((BrooklynObjectInternal.ConfigurationSupportInternal)l.config()).getRaw(MY_CONFIG_KEY);
        Assert.assertTrue(rawConfig.isPresentAndNonNull());
        Assert.assertTrue(rawConfig.get() instanceof DeferredSupplier, "Expected deferred raw value; got "+rawConfig.get());
    }
    
    @Test(groups="Integration")
    public void testLocationFromCatalogInheritanceAndRebind() throws Exception {
        ImmutableList.Builder<String> yamlL = ImmutableList.<String>builder().add(
                "brooklyn.catalog:",
                "  id: l1",
                "  item.type: location",
                "  item:",
                "    type: localhost",
                "    brooklyn.config:",
                "      simple: 42",
                "      my.config.key: $brooklyn:external(\"myprovider\", \"mykey\")");
        addCatalogItems(yamlL.build());

        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: "+EmptySoftwareProcess.class.getName(),
                "location: l1");

        origApp = (StartableApplication) createAndStartApplication(new StringReader(yaml));

        Entity entity = Iterables.getOnlyElement( origApp.getChildren() );
        Location l = Iterables.getOnlyElement( entity.getLocations() );
        assertEquals(l.config().get(ConfigKeys.builder(Integer.class, "simple").build()), (Integer)42);
        assertEquals(l.config().get(MY_CONFIG_KEY), "myval");
                
        Maybe<Object> rawConfig = ((BrooklynObjectInternal.ConfigurationSupportInternal)l.config()).getRaw(MY_CONFIG_KEY);
        Assert.assertTrue(rawConfig.isPresentAndNonNull());
        Assert.assertTrue(rawConfig.get() instanceof DeferredSupplier, "Expected deferred raw value; got "+rawConfig.get());
        
        newApp = rebind();
        
        entity = Iterables.getOnlyElement( newApp.getChildren() );
        l = Iterables.getOnlyElement( entity.getLocations() );
        assertEquals(l.config().get(ConfigKeys.builder(Integer.class, "simple").build()), (Integer)42);
        assertEquals(l.config().get(MY_CONFIG_KEY), "myval");
                
        rawConfig = ((BrooklynObjectInternal.ConfigurationSupportInternal)l.config()).getRaw(MY_CONFIG_KEY);
        Assert.assertTrue(rawConfig.isPresentAndNonNull());
        Assert.assertTrue(rawConfig.get() instanceof DeferredSupplier, "Expected deferred raw value; got "+rawConfig.get());
    }
    
    @Test(groups="Integration")
    public void testProvisioningPropertyInheritance() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: "+EmptySoftwareProcess.class.getName(),
                "  provisioning.properties:",
                "      simple: 42",
                "      my.config.key: $brooklyn:external(\"myprovider\", \"mykey\")",
                "location: localhost");

        origApp = (StartableApplication) createAndStartApplication(new StringReader(yaml));

        Entity entity = Iterables.getOnlyElement( origApp.getChildren() );
        Location l = Iterables.getOnlyElement( entity.getLocations() );
        assertEquals(l.config().get(ConfigKeys.builder(Integer.class, "simple").build()), (Integer)42);
        assertEquals(l.config().get(MY_CONFIG_KEY), "myval");
                
        Maybe<Object> rawConfig = ((BrooklynObjectInternal.ConfigurationSupportInternal)l.config()).getRaw(MY_CONFIG_KEY);
        Assert.assertTrue(rawConfig.isPresentAndNonNull());
        Assert.assertTrue(rawConfig.get() instanceof DeferredSupplier, "Expected deferred raw value; got "+rawConfig.get());
    }

}
