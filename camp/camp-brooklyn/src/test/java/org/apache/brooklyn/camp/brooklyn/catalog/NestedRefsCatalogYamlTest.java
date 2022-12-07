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

import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.core.mgmt.classloading.OsgiBrooklynClassLoadingContext;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Map;


public class NestedRefsCatalogYamlTest extends AbstractYamlTest {

    private static final Logger LOG = LoggerFactory.getLogger(NestedRefsCatalogYamlTest.class);

    @Override
    protected boolean disableOsgi() {
        return false;
    }

    @Test
    public void testPreferHighestVersion() {
        addCatalogItems("brooklyn.catalog:\n" +
                "  bundle: Bv2\n" +
                "  version: \"2\"\n" +
                "  items:\n" +
                "    - id: A\n" +
                "      item:\n" +
                "        name: Av2\n" +
                "        type: org.apache.brooklyn.entity.stock.BasicEntity\n");
        addCatalogItems("brooklyn.catalog:\n" +
                "  bundle: Bv1\n" +
                "  version: \"1\"\n" +
                "  items:\n" +
                "    - id: A\n" +
                "      item:\n" +
                "        name: Av1\n" +
                "        type: org.apache.brooklyn.entity.stock.BasicEntity\n");

        AbstractBrooklynObjectSpec<?, ?> x = mgmt().getTypeRegistry().createSpec(
                mgmt().getTypeRegistry().get("A"), null, null);

        Asserts.assertEquals(x.getDisplayName(), "Av2");
    }

    @Test
    public void testPreferSameBundle() {
        addCatalogItems("brooklyn.catalog:\n" +
                "  bundle: Bv2\n" +
                "  version: \"2\"\n" +
                "  items:\n" +
                "    - id: A\n" +
                "      item:\n" +
                "        name: Av2\n" +
                "        type: org.apache.brooklyn.entity.stock.BasicEntity\n");
        addCatalogItems("brooklyn.catalog:\n" +
                "  bundle: Bv1\n" +
                "  version: \"1\"\n" +
                "  items:\n" +
                "    - id: A\n" +
                "      item:\n" +
                "        name: Av1\n" +
                "        type: org.apache.brooklyn.entity.stock.BasicEntity\n" +
                "    - id: X\n" +
                "      item:\n" +
                "        type: A");
        AbstractBrooklynObjectSpec<?, ?> x = mgmt().getTypeRegistry().createSpec(
                mgmt().getTypeRegistry().get("X"), null, null);

        Asserts.assertEquals(x.getDisplayName(), "Av1");
    }

    @Test
    public void testPreferSameBundleComplex() {
        addCatalogItems("brooklyn.catalog:\n" +
                "  bundle: Bv2\n" +
                "  version: \"2\"\n" +
                "  items:\n" +
                "    - id: A\n" +
                "      item:\n" +
                "        name: Av2\n" +
                "        type: org.apache.brooklyn.entity.stock.BasicEntity\n"+
                "    - id: Z\n" +
                "      item:\n" +
                "        name: Z\n" +
                "        type: org.apache.brooklyn.entity.stock.BasicEntity\n");
        addCatalogItems("brooklyn.catalog:\n" +
                "  bundle: Bv1\n" +
                "  version: \"1\"\n" +
                "  items:\n" +
                "    - id: A\n" +
                "      item:\n" +
                "        name: Av1\n" +
                "        type: Z\n" +
                "    - id: X\n" +
                "      item:\n" +
                "        type: A");
        AbstractBrooklynObjectSpec<?, ?> x = mgmt().getTypeRegistry().createSpec(
                mgmt().getTypeRegistry().get("X"), null, null);

        Asserts.assertEquals(x.getDisplayName(), "Av1");
    }

    /* test that invoke-effector doesn't cause confusion */
    void doTestAddAfterWorkflow(String url) {
        // add invoke-effector bean via a bundle
//        WorkflowBasicTest.addWorkflowStepTypes(mgmt());
        Collection<RegisteredType> typesW = addCatalogItems(ResourceUtils.create(this).getResourceAsString("classpath://nested-refs-catalog-yaml-workflow-test.bom.yaml"));

        // now add it as a type, in various ways
        Collection<RegisteredType> types = addCatalogItems(ResourceUtils.create(this).getResourceAsString(url));

        // should prefer from local bundle
        Asserts.assertEquals(mgmt().getTypeRegistry().get("invoke-effector", RegisteredTypeLoadingContexts.loader(
                new OsgiBrooklynClassLoadingContext(mgmt(), types.iterator().next().getId(), null))).getContainingBundle(), "test-bundle:0.1.0-SNAPSHOT");

        // re-do the validation to be sure
        Map<RegisteredType, Collection<Throwable>> result = mgmt().getCatalog().validateTypes(types);
        if (!result.isEmpty()) {
            Asserts.fail("Failed validation should be empty: " + result);
        }
    }

    @Test
    public void testAddAmbiguousCatalogItemsPreferEntitySpecAsParent() throws Exception {
        doTestAddAfterWorkflow("classpath://nested-refs-catalog-yaml-test.bom.yaml");
    }

    @Test
    public void testAddAmbiguousCatalogItemsPreferFromSameBundle() throws Exception {
        doTestAddAfterWorkflow("classpath://nested-refs-catalog-yaml-test-2.bom.yaml");
    }

    @Test  // allow validation to change the kind once all peers are loaded
    public void testAddAmbiguousCatalogItemsPreferFromSameBundleEvenIfDefinedLater() throws Exception {
        doTestAddAfterWorkflow("classpath://nested-refs-catalog-yaml-test-3.bom.yaml");
    }

}
