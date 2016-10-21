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
import java.util.Map;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.camp.brooklyn.ExternalConfigYamlTest.MyExternalConfigSupplier;
import org.apache.brooklyn.camp.brooklyn.ExternalConfigYamlTest.MyExternalConfigSupplierWithoutMapArg;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.StartableApplication;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

// also see ExternalConfigYamlTest
public class JcloudsExternalConfigYamlTest extends AbstractJcloudsStubYamlTest {

    private static final Logger log = LoggerFactory.getLogger(JcloudsExternalConfigYamlTest.class);

    private static final ConfigKey<String> MY_CONFIG_KEY = ConfigKeys.newStringConfigKey("my.config.key");
    
    @Override
    protected Map<String, ?> customBrooklynProperties() {
        return MutableMap.<String, Object>builder()
                .putAll(super.customBrooklynProperties())
                .put("brooklyn.external.myprovider", MyExternalConfigSupplier.class.getName())
                .put("brooklyn.external.myprovider.mykey", "myval")
                .put("brooklyn.external.myproviderWithoutMapArg", MyExternalConfigSupplierWithoutMapArg.class.getName())
                .build();
    }

    @Test
    public void testJcloudsInheritanceAndPasswordSecret() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location:",
                "  " + LOCATION_CATALOG_ID + ":",
                "    password: $brooklyn:external(\"myprovider\", \"mykey\")",
                "    my.config.key: $brooklyn:external(\"myprovider\", \"mykey\")",
                "services:",
                "- type: "+EmptySoftwareProcess.class.getName());

        Application app = (StartableApplication) createAndStartApplication(new StringReader(yaml));

        Entity entity = Iterables.getOnlyElement( app.getChildren() );
        Location l = Iterables.getOnlyElement( entity.getLocations() );
        log.info("Location: "+l);
        assertEquals(l.config().get(MY_CONFIG_KEY), "myval");

        Maybe<Object> rawConfig = ((BrooklynObjectInternal.ConfigurationSupportInternal)l.config()).getRaw(MY_CONFIG_KEY);
        log.info("Raw config: "+rawConfig);
        Assert.assertTrue(rawConfig.isPresentAndNonNull());
        Assert.assertTrue(rawConfig.get() instanceof DeferredSupplier, "Expected deferred raw value; got "+rawConfig.get());

        rawConfig = ((BrooklynObjectInternal.ConfigurationSupportInternal)l.config()).getRaw(SshTool.PROP_PASSWORD);
        log.info("Raw config password: "+rawConfig);
        Assert.assertTrue(rawConfig.isPresentAndNonNull());
        Assert.assertTrue(rawConfig.get() instanceof DeferredSupplier, "Expected deferred raw value; got "+rawConfig.get());
    }

    @Test
    public void testProvisioningPropertyInheritance() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location: " + LOCATION_CATALOG_ID,
                "services:",
                "- type: "+EmptySoftwareProcess.class.getName(),
                "  provisioning.properties:",
                "      password: $brooklyn:external(\"myprovider\", \"mykey\")",
                // note that these 2 do not get transferred -- see below
                "      simple: 42",
                "      my.config.key: $brooklyn:external(\"myprovider\", \"mykey\")");

        StartableApplication app = (StartableApplication) createAndStartApplication(new StringReader(yaml));

        Entity entity = Iterables.getOnlyElement( app.getChildren() );
        Location l = Iterables.getOnlyElement( entity.getLocations() );
        log.info("Location: "+l);
        assertEquals(l.config().get(JcloudsLocation.PASSWORD), "myval");

        Maybe<Object> rawConfig = ((BrooklynObjectInternal.ConfigurationSupportInternal)l.config()).getRaw(ConfigKeys.newStringConfigKey("password"));
        log.info("Raw config password: "+rawConfig);
        Assert.assertTrue(rawConfig.isPresentAndNonNull());
        Assert.assertTrue(rawConfig.get() instanceof DeferredSupplier, "Expected deferred raw value; got "+rawConfig.get());

        // these are null as only recognised provisioning properties are transmitted by jclouds
        log.info("my config key: "+l.getConfig(MY_CONFIG_KEY));
        log.info("my config key raw: "+((BrooklynObjectInternal.ConfigurationSupportInternal)l.config()).getRaw(MY_CONFIG_KEY));
        log.info("simple: "+l.getConfig(ConfigKeys.builder(Integer.class, "simple").build()));
        log.info("simple raw: "+((BrooklynObjectInternal.ConfigurationSupportInternal)l.config()).getRaw(ConfigKeys.builder(Integer.class, "simple").build()));
    }
}