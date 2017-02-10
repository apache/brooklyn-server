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

import java.util.List;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.core.mgmt.BrooklynTags.NamedStringTag;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.core.mgmt.osgi.OsgiStandaloneTest;
import org.apache.brooklyn.core.typereg.RegisteredTypePredicates;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;


public class CatalogOsgiYamlTemplateTest extends AbstractYamlTest {
    
    private static final String SIMPLE_ENTITY_TYPE = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY;

    @Override
    protected boolean disableOsgi() {
        return false;
    }

    @Test
    public void testAddCatalogItemOsgi() throws Exception {
        RegisteredType item = makeItem("t1", SIMPLE_ENTITY_TYPE);
        Assert.assertTrue(RegisteredTypePredicates.IS_APPLICATION.apply(item), "item: "+item);
        String yaml = RegisteredTypes.getImplementationDataStringForSpec(item);
        Assert.assertTrue(yaml.indexOf("sample comment")>=0,
            "YAML did not include original comments; it was:\n"+yaml);
        Assert.assertFalse(yaml.indexOf("description")>=0,
            "YAML included metadata which should have been excluded; it was:\n"+yaml);

        // Confirm can deploy an app using this template catalog item
        Entity app = createAndStartApplication(
                "services:",
                "- type: t1");
        waitForApplicationTasks(app);
        
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(entity.getEntityType().getName(), SIMPLE_ENTITY_TYPE);
        
        deleteCatalogEntity("t1");
    }

    @Test
    public void testMetadataOnSpecCreatedFromItem() throws Exception {
        makeItem("t1", SIMPLE_ENTITY_TYPE);
        EntitySpec<? extends Application> spec = EntityManagementUtils.createEntitySpecForApplication(mgmt(), 
            "services: [ { type: t1 } ]\n" +
            "location: localhost");
        
        List<NamedStringTag> yamls = BrooklynTags.findAll(BrooklynTags.YAML_SPEC_KIND, spec.getTags());
        Assert.assertEquals(yamls.size(), 1, "Expected 1 yaml tag; instead had: "+yamls);
        String yaml = Iterables.getOnlyElement(yamls).getContents();
        Asserts.assertStringContains(yaml, "services:", "t1", "localhost");
        
        EntitySpec<?> child = Iterables.getOnlyElement( spec.getChildren() );
        Assert.assertEquals(child.getType().getName(), SIMPLE_ENTITY_TYPE);
        Assert.assertEquals(child.getCatalogItemId(), "t1:"+TEST_VERSION);
    }
    
    private RegisteredType makeItem(String symbolicName, String templateType) {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);
        
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  itemType: template",
            "  name: My Catalog App",
            "  description: My description",
            "  icon_url: classpath://path/to/myicon.jpg",
            "  version: " + TEST_VERSION,
            "  libraries:",
            "  - url: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL,
            "  item:",
            "    services:",
            "    # this sample comment should be included",
            "    - type: " + templateType);

        return mgmt().getTypeRegistry().get(symbolicName, TEST_VERSION);
    }
}
