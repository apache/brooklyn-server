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
package org.apache.brooklyn.camp.brooklyn.catalog;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.objs.SpecParameter;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.objs.BasicSpecParameter;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;

public class SpecParameterParsingOsgiTest extends AbstractYamlTest {

    private static final int NUM_APP_DEFAULT_CONFIG_KEYS = SpecParameterUnwrappingTest.NUM_APP_DEFAULT_CONFIG_KEYS;
    
    @Override
    protected boolean disableOsgi() {
        return false;
    }

    @Test
    public void testOsgiType() {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String itemId = ver("test.inputs", "0.0.1");
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: test.inputs",
                "  version: 0.0.1",
                "  itemType: entity",
                "  libraries:",
                "  - classpath://" + OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_PATH,
                "  item: ",
                "    type: "+ BasicApplication.class.getName(),
                "    brooklyn.parameters:",
                "    - name: simple",
                "      type: " + OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY);
        AbstractBrooklynObjectSpec<?,?> spec = createSpec(itemId);
        List<SpecParameter<?>> inputs = spec.getParameters();
        assertEquals(inputs.size(), NUM_APP_DEFAULT_CONFIG_KEYS + 1, "inputs="+inputs);
        SpecParameter<?> firstInput = inputs.get(0);
        assertEquals(firstInput.getLabel(), "simple");
        assertTrue(firstInput.isPinned());
        assertEquals(firstInput.getConfigKey().getName(), "simple");
        assertEquals(firstInput.getConfigKey().getTypeToken().getRawType().getName(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY);
    }

    @Test
    public void testOsgiClassScanned() {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_PATH);
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_MORE_ENTITIES_V2_PATH);
        addCatalogItems(CatalogScanOsgiTest.bomForLegacySiblingLibraries());
        
        RegisteredType item = mgmt().getTypeRegistry().get(OsgiTestResources.BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY);
        AbstractBrooklynObjectSpec<?,?> spec = createSpec(item);
        List<SpecParameter<?>> inputs = spec.getParameters();
        if (inputs.isEmpty()) Assert.fail("no inputs (if you're in the IDE, mvn clean install may need to be run to rebuild osgi test JARs)");
        
        Set<SpecParameter<?>> actual = ImmutableSet.copyOf(inputs);
        Set<SpecParameter<?>> expected = ImmutableSet.<SpecParameter<?>>of(
                new BasicSpecParameter<>("more_config", false, ConfigKeys.newStringConfigKey("more_config")),
                new BasicSpecParameter<>(AbstractEntity.DEFAULT_DISPLAY_NAME.getName(), false, AbstractEntity.DEFAULT_DISPLAY_NAME));
        assertEquals(actual, expected);
    }

    private AbstractBrooklynObjectSpec<?, ?> createSpec(String itemId) {
        RegisteredType item = mgmt().getTypeRegistry().get(itemId);
        Assert.assertNotNull(item, "Could not load: "+itemId);
        return createSpec(item);
    }
    
    private AbstractBrooklynObjectSpec<?, ?> createSpec(RegisteredType item) {
        return mgmt().getTypeRegistry().createSpec(item, null, EntitySpec.class);
    }

}
