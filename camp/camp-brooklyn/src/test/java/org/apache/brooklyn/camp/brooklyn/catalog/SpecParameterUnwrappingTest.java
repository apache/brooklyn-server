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

import static org.apache.brooklyn.core.objs.SpecParameterPredicates.labelEqualTo;
import static org.apache.brooklyn.core.objs.SpecParameterPredicates.nameEqualTo;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.SpecParameter;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.ConfigPredicates;
import org.apache.brooklyn.core.entity.AbstractApplication;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.objs.BasicSpecParameter;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicStartable;

public class SpecParameterUnwrappingTest extends AbstractYamlTest {
    private static final String SYMBOLIC_NAME = "my.catalog.app.id.load";

    private static final ConfigKey<String> SHARED_CONFIG = ConfigKeys.newStringConfigKey("sample.config");
    public static class ConfigAppForTest extends AbstractApplication {
        public static final ConfigKey<String> SAMPLE_CONFIG = SHARED_CONFIG;
    }
    public static class ConfigEntityForTest extends AbstractEntity {
        public static final ConfigKey<String> SAMPLE_CONFIG = SHARED_CONFIG;
    }
    public static class ConfigPolicyForTest extends AbstractPolicy {
        public static final ConfigKey<String> SAMPLE_CONFIG = SHARED_CONFIG;
    }
    public static class ConfigLocationForTest extends AbstractLocation {
        public static final ConfigKey<String> SAMPLE_CONFIG = SHARED_CONFIG;
    }

    @Override
    protected LocalManagementContext newTestManagementContext() {
        // Don't need OSGi
        return LocalManagementContextForTests.newInstance();
    }

    @DataProvider(name="brooklynTypes")
    public Object[][] brooklynTypes() {
        return new Object[][] {
            {ConfigEntityForTest.class},
            {ConfigPolicyForTest.class},
            {ConfigLocationForTest.class}};
    }

    @Test(dataProvider = "brooklynTypes")
    public void testParameters(Class<? extends BrooklynObject> testClass) {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + SYMBOLIC_NAME,
                "  version: " + TEST_VERSION,
                "  itemType: " + inferItemType(testClass),
                "  item:",
                "    type: " + testClass.getName(),
                "    brooklyn.parameters:",
                "    - simple");

        ConfigKey<String> SIMPLE_CONFIG = ConfigKeys.newStringConfigKey("simple");
        SpecParameter<String> SIMPLE_PARAM = new BasicSpecParameter<>("simple", true, SIMPLE_CONFIG);
        CatalogItem<?, ?> item = catalog.getCatalogItem(SYMBOLIC_NAME, TEST_VERSION);
        AbstractBrooklynObjectSpec<?,?> spec = createSpec(item);
        assertTrue(Iterables.tryFind(spec.getParameters(), Predicates.<SpecParameter<?>>equalTo(SIMPLE_PARAM)).isPresent());
    }

    @Test(dataProvider = "brooklynTypes")
    public void testDefaultParameters(Class<? extends BrooklynObject> testClass) {
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + SYMBOLIC_NAME,
            "  version: " + TEST_VERSION,
            "  itemType: " + inferItemType(testClass),
            "  item:",
            "    type: "+ testClass.getName());

        CatalogItem<?, ?> item = catalog.getCatalogItem(SYMBOLIC_NAME, TEST_VERSION);
        AbstractBrooklynObjectSpec<?, ?> spec = createSpec(item);
        assertEquals(ImmutableSet.copyOf(spec.getParameters()), ImmutableSet.copyOf(BasicSpecParameter.fromClass(mgmt(),testClass)));
    }

    @Test
    public void testRootParametersUnwrapped() {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + SYMBOLIC_NAME,
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  item:",
                "    services:",
                "    - type: " + ConfigEntityForTest.class.getName(),
                "    brooklyn.parameters:",
                "    - simple");

        CatalogItem<?, ?> item = catalog.getCatalogItem(SYMBOLIC_NAME, TEST_VERSION);
        AbstractBrooklynObjectSpec<?,?> spec = createSpec(item);
        List<SpecParameter<?>> params = spec.getParameters();
        assertEquals(params.size(), 4);
        assertTrue(Iterables.tryFind(params, nameEqualTo("simple")).isPresent());
    }

    @Test(dataProvider="brooklynTypes")
    public void testDependantCatalogsInheritParameters(Class<? extends BrooklynObject> type) {
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: " + TEST_VERSION,
                "  itemType: " + inferItemType(type),
                "  items:",
                "  - id: paramItem",
                "    item:",
                "      type: " + type.getName(),
                "      brooklyn.parameters:",
                "      - simple",
                "  - id: " + SYMBOLIC_NAME,
                "    item:",
                "      type: paramItem");

        CatalogItem<?, ?> item = catalog.getCatalogItem(SYMBOLIC_NAME, TEST_VERSION);
        AbstractBrooklynObjectSpec<?,?> spec = createSpec(item);
        List<SpecParameter<?>> params = spec.getParameters();
        switch (type.getSimpleName()) {
            case "ConfigEntityForTest":
                assertEquals(params.size(), 3);
                break;
            case "ConfigPolicyForTest":
                assertEquals(params.size(), 2);
                break;
            case "ConfigLocationForTest":
                assertEquals(params.size(), 7);
                break;
        }
        assertTrue(Iterables.tryFind(params, nameEqualTo("simple")).isPresent());
        assertTrue(Iterables.tryFind(params, labelEqualTo("simple")).isPresent());
    }

    @Test(dataProvider="brooklynTypes")
    public void testDependantCatalogsExtendsParameters(Class<? extends BrooklynObject> type) {
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: " + TEST_VERSION,
                "  itemType: " + inferItemType(type),
                "  items:",
                "  - id: paramItem",
                "    item:",
                "      type: " + type.getName(),
                "      brooklyn.parameters:",
                "      - simple",
                "  - id: " + SYMBOLIC_NAME,
                "    item:",
                "      type: paramItem",
                "      brooklyn.parameters:",
                "      - override");

        CatalogItem<?, ?> item = catalog.getCatalogItem(SYMBOLIC_NAME, TEST_VERSION);
        AbstractBrooklynObjectSpec<?,?> spec = createSpec(item);
        List<SpecParameter<?>> params = spec.getParameters();
        switch (type.getSimpleName()) {
            case "ConfigEntityForTest":
                assertEquals(params.size(), 4);
                break;
            case "ConfigPolicyForTest":
                assertEquals(params.size(), 3);
                break;
            case "ConfigLocationForTest":
                assertEquals(params.size(), 8);
                break;
        }
        assertTrue(Iterables.tryFind(params, nameEqualTo("simple")).isPresent());
        assertTrue(Iterables.tryFind(params, nameEqualTo("override")).isPresent());
    }

    @Test(dataProvider="brooklynTypes")
    public void testDependantCatalogMergesParameters(Class<? extends BrooklynObject> type) {
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: " + TEST_VERSION,
                "  items:",
                "  - id: paramItem",
                "    item:",
                "      type: " + type.getName(),
                "      brooklyn.parameters:",
                "      - name: simple",
                "        label: simple",
                "  - id: " + SYMBOLIC_NAME,
                "    item:",
                "      type: paramItem",
                "      brooklyn.parameters:",
                "      - name: simple",
                "        label: override");

        CatalogItem<?, ?> item = catalog.getCatalogItem(SYMBOLIC_NAME, TEST_VERSION);
        AbstractBrooklynObjectSpec<?,?> spec = createSpec(item);
        List<SpecParameter<?>> params = spec.getParameters();
        switch (type.getSimpleName()) {
            case "ConfigEntityForTest":
                assertEquals(params.size(), 4);
                break;
            case "ConfigPolicyForTest":
                assertEquals(params.size(), 3);
                break;
            case "ConfigLocationForTest":
                assertEquals(params.size(), 8);
                break;
        }
        assertTrue(Iterables.tryFind(params, nameEqualTo("simple")).isPresent());
        assertTrue(Iterables.tryFind(params, labelEqualTo("override")).isPresent());
    }

    @Test
    public void testDependantCatalogConfigOverridesParameters() {
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: " + TEST_VERSION,
                "  items:",
                "  - id: paramItem",
                "    item:",
                "      type: " + ConfigEntityForTest.class.getName(),
                "      brooklyn.parameters:",
                "      - name: simple",
                "        default: biscuits",
                "      brooklyn.config:",
                "        simple: value",
                "  - id: " + SYMBOLIC_NAME,
                "    item:",
                "      type: paramItem",
                "      brooklyn.parameters:",
                "      - name: simple",
                "        default: rabbits");

        CatalogItem<?, ?> item = catalog.getCatalogItem(SYMBOLIC_NAME, TEST_VERSION);
        AbstractBrooklynObjectSpec<?,?> spec = createSpec(item);
        List<SpecParameter<?>> params = spec.getParameters();
        assertTrue(Iterables.tryFind(params, nameEqualTo("simple")).isPresent());
        Optional<ConfigKey<?>> config = Iterables.tryFind(spec.getConfig().keySet(), ConfigPredicates.nameEqualTo("simple"));
        assertTrue(config.isPresent());
        Object value = spec.getConfig().get(config.get());
        assertEquals(value, "value");
    }

    @Test
    public void testCatalogConfigOverridesParameters() {
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: " + TEST_VERSION,
                "  items:",
                "  - id: " + SYMBOLIC_NAME,
                "    item:",
                "      type: " + ConfigEntityForTest.class.getName(),
                "      brooklyn.parameters:",
                "      - name: simple",
                "        default: biscuits",
                "      brooklyn.config:",
                "        simple: value");

        CatalogItem<?, ?> item = catalog.getCatalogItem(SYMBOLIC_NAME, TEST_VERSION);
        AbstractBrooklynObjectSpec<?,?> spec = createSpec(item);
        List<SpecParameter<?>> params = spec.getParameters();
        assertEquals(params.size(), 3);
        assertTrue(Iterables.tryFind(params, nameEqualTo("simple")).isPresent());
        Optional<ConfigKey<?>> config = Iterables.tryFind(spec.getConfig().keySet(), ConfigPredicates.nameEqualTo("simple"));
        assertTrue(config.isPresent());
        Object value = spec.getConfig().get(config.get());
        assertEquals(value, "value");
    }

    @Test
    public void testDependantCatalogConfigReplacesParameters() {
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: " + TEST_VERSION,
                "  items:",
                "  - id: paramItem",
                "    item:",
                "      type: " + ConfigEntityForTest.class.getName(),
                "      brooklyn.config:",
                "        simple: value",
                "  - id: " + SYMBOLIC_NAME,
                "    item:",
                "      type: paramItem",
                "      brooklyn.parameters:",
                "      - name: simple",
                "        default: rabbits");

        CatalogItem<?, ?> item = catalog.getCatalogItem(SYMBOLIC_NAME, TEST_VERSION);
        AbstractBrooklynObjectSpec<?,?> spec = createSpec(item);
        List<SpecParameter<?>> params = spec.getParameters();
        assertTrue(Iterables.tryFind(params, nameEqualTo("simple")).isPresent());
        Optional<ConfigKey<?>> config = Iterables.tryFind(spec.getConfig().keySet(), ConfigPredicates.nameEqualTo("simple"));
        assertTrue(config.isPresent());
        Object value = spec.getConfig().get(config.get());
        assertEquals(value, "value");
    }

    @Test
    public void testChildEntitiyHasParameters() {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + SYMBOLIC_NAME,
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  item:",
                "    type: " + ConfigEntityForTest.class.getName(),
                "    brooklyn.children:",
                "    - type: " + ConfigEntityForTest.class.getName(),
                "      brooklyn.parameters:",
                "      - simple");

        CatalogItem<?, ?> item = catalog.getCatalogItem(SYMBOLIC_NAME, TEST_VERSION);
        @SuppressWarnings({ "rawtypes", "unchecked"})
        EntitySpec<?> parentSpec = (EntitySpec<?>) catalog.createSpec((CatalogItem)item);
        EntitySpec<?> spec = parentSpec.getChildren().get(0);
        List<SpecParameter<?>> params = spec.getParameters();
        assertEquals(params.size(), 3);
        assertTrue(Iterables.tryFind(params, nameEqualTo("simple")).isPresent());
        assertTrue(Iterables.tryFind(params, labelEqualTo("simple")).isPresent());
    }

    @Test
    public void testAppSpecInheritsCatalogParameters() {
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: " + TEST_VERSION,
                "  id: " + SYMBOLIC_NAME,
                "  itemType: entity",
                "  item:",
                "    type: " + BasicApplication.class.getName(),
                "    brooklyn.parameters:",
                "    - simple");

        EntitySpec<? extends Application> spec = createAppSpec(
                "services:",
                "- type: " + ver(SYMBOLIC_NAME));
        List<SpecParameter<?>> params = spec.getParameters();
        assertEquals(params.size(), 4);
        assertTrue(Iterables.tryFind(params, nameEqualTo("simple")).isPresent());
        assertTrue(Iterables.tryFind(params, labelEqualTo("simple")).isPresent());
    }


    @Test
    public void testAppSpecInheritsCatalogRootParameters() {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + SYMBOLIC_NAME,
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  item:",
                "    type: " + BasicApplication.class.getName(),
                "    brooklyn.parameters:",
                "    - simple");

        EntitySpec<? extends Application> spec = createAppSpec(
                "services:",
                "- type: " + ver(SYMBOLIC_NAME));
        List<SpecParameter<?>> params = spec.getParameters();
        assertEquals(params.size(), 4);
        assertTrue(Iterables.tryFind(params, nameEqualTo("simple")).isPresent());
        assertTrue(Iterables.tryFind(params, labelEqualTo("simple")).isPresent());
    }

    @Test
    public void testAppSpecInheritsCatalogRootParametersWithServices() {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + SYMBOLIC_NAME,
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  item:",
                "    brooklyn.parameters:",
                "    - simple",
                "    services:",
                "    - type: " + BasicApplication.class.getName());

        EntitySpec<? extends Application> spec = createAppSpec(
                "services:",
                "- type: " + ver(SYMBOLIC_NAME));
        List<SpecParameter<?>> params = spec.getParameters();
        assertEquals(params.size(), 4);
        assertTrue(Iterables.tryFind(params, nameEqualTo("simple")).isPresent());
        assertTrue(Iterables.tryFind(params, labelEqualTo("simple")).isPresent());
    }

    @Test
    public void testUnresolvedCatalogItemParameters() {
        // Insert template which is not instantiatable during catalog addition due to
        // missing dependencies, but the spec can be created (the
        // dependencies are already parsed).
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: " + TEST_VERSION,
                "  items:",
                "  - id: " + SYMBOLIC_NAME,
                "    itemType: template",
                "    item:",
                "      services:",
                "      - type: basic-app",
                "  - id: basic-app",
                "    itemType: entity",
                "    item:",
                "      type: " + ConfigAppForTest.class.getName());
        EntitySpec<? extends Application> spec = createAppSpec(
                "services:",
                "- type: " + ver(SYMBOLIC_NAME));
        List<SpecParameter<?>> params = spec.getParameters();
        assertEquals(params.size(), 4, "Wrong number of params: "+params); // sample + defaultDisplayName + destroy_on_stop + start.latch
        assertEquals(ImmutableSet.copyOf(params), ImmutableSet.copyOf(BasicSpecParameter.fromClass(mgmt(), ConfigAppForTest.class)));
    }

    @Test
    public void testParametersCoercedOnSetAndReferences() throws Exception {
        Integer testValue = Integer.valueOf(55);
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + SYMBOLIC_NAME,
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  item:",
                "    type: " + BasicApplication.class.getName(),
                "    brooklyn.parameters:",
                "    - name: num",
                "      type: integer",
                "    brooklyn.children:",
                "    - type: " + ConfigEntityForTest.class.getName(),
                "      brooklyn.config:",
                "        refConfig: $brooklyn:scopeRoot().config(\"num\")",
                "    - type: " + ConfigEntityForTest.class.getName(),
                "      brooklyn.config:",
                "        refConfig: $brooklyn:config(\"num\")"); //inherited config

        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.children:",
                "  - type: " + ver(SYMBOLIC_NAME),
                "    brooklyn.config:",
                "      num: \"" + testValue + "\"");

        Entity scopeRoot = Iterables.getOnlyElement(app.getChildren());

        ConfigKey<Object> numKey = ConfigKeys.newConfigKey(Object.class, "num");
        assertEquals(scopeRoot.config().get(numKey), testValue);

        ConfigKey<Object> refConfigKey = ConfigKeys.newConfigKey(Object.class, "refConfig");

        Iterator<Entity> childIter = scopeRoot.getChildren().iterator();
        Entity c1 = childIter.next();
        assertEquals(c1.config().get(refConfigKey), testValue);
        Entity c2 = childIter.next();
        assertEquals(c2.config().get(refConfigKey), testValue);
        assertFalse(childIter.hasNext());
    }

    private static final ConfigKey<Integer> NUM = ConfigKeys.newIntegerConfigKey("num");

    @Test
    public void testParameterDefaultsUsedInConfig() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: " + TEST_VERSION,
                "  items:",
                "    - id: " + ConfigEntityForTest.class.getSimpleName() + "WithParams",
                "      itemType: entity",
                "      item:",
                "        type: " + ConfigEntityForTest.class.getName(),
                "        brooklyn.parameters:",
                "          - name: num",
                "            type: integer",
                "            default: 1234",
                "        brooklyn.children:",
                "          - type: " + BasicStartable.class.getName(),
                "            name: s",
                "            brooklyn.config:",
                "              test: $brooklyn:parent().config(\"num\")",
                "    - id: " + SYMBOLIC_NAME,
                "      itemType: entity",
                "      item:",
                "        type: " + BasicApplication.class.getName(),
                "        brooklyn.children:",
                "          - type: " + ConfigEntityForTest.class.getSimpleName() + "WithParams",
                "            name: a",
                "          - type: " + ConfigEntityForTest.class.getSimpleName() + "WithParams",
                "            name: b",
                "            brooklyn.config:",
                "              num: 5678",
                "          - type: " + ConfigEntityForTest.class.getSimpleName() + "WithParams",
                "            name: c",
                "            brooklyn.config:",
                "              test: $brooklyn:config(\"num\")");

        CatalogItem<?, ?> item = catalog.getCatalogItem(ConfigEntityForTest.class.getSimpleName() + "WithParams", TEST_VERSION);
        AbstractBrooklynObjectSpec<?,?> spec = createSpec(item);
        List<SpecParameter<?>> params = spec.getParameters();
        assertEquals(params.size(), 3);
        assertTrue(Iterables.tryFind(params, nameEqualTo("num")).isPresent());
        
        Application app = (Application) createAndStartApplication(
                "services:",
                "  - type: " + ver(SYMBOLIC_NAME));

        Iterable<Entity> children = app.getChildren();
        Optional<Entity> a = Iterables.tryFind(children, EntityPredicates.displayNameEqualTo("a"));
        assertTrue(a.isPresent());
        assertEquals(a.get().config().get(NUM).intValue(), 1234);
        Optional<Entity> as = Iterables.tryFind(a.get().getChildren(), EntityPredicates.displayNameEqualTo("s"));
        assertTrue(as.isPresent());
        assertEquals(as.get().config().get(ConfigKeys.newIntegerConfigKey("test")).intValue(), 1234);
        Optional<Entity> b = Iterables.tryFind(children, EntityPredicates.displayNameEqualTo("b"));
        assertTrue(b.isPresent());
        assertEquals(b.get().config().get(NUM).intValue(), 5678);
        Optional<Entity> bs = Iterables.tryFind(b.get().getChildren(), EntityPredicates.displayNameEqualTo("s"));
        assertTrue(bs.isPresent());
        assertEquals(bs.get().config().get(ConfigKeys.newIntegerConfigKey("test")).intValue(), 5678);
        Optional<Entity> c = Iterables.tryFind(children, EntityPredicates.displayNameEqualTo("c"));
        assertTrue(c.isPresent());
        assertEquals(c.get().config().get(ConfigKeys.newIntegerConfigKey("test")).intValue(), 1234);
        Optional<Entity> cs = Iterables.tryFind(c.get().getChildren(), EntityPredicates.displayNameEqualTo("s"));
        assertTrue(cs.isPresent());
        assertEquals(cs.get().config().get(ConfigKeys.newIntegerConfigKey("test")).intValue(), 1234);
    }

    @Test
    public void testAppRootParameters() throws Exception {
        EntitySpec<? extends Application> spec = createAppSpec(
                "brooklyn.parameters:",
                "- simple",
                "services:",
                "- type: " + BasicApplication.class.getName());
        List<SpecParameter<?>> inputs = spec.getParameters();
        assertEquals(inputs.size(), 4);
        SpecParameter<?> firstInput = inputs.get(0);
        assertEquals(firstInput.getLabel(), "simple");
    }

    @Test
    public void testAppServiceParameters() throws Exception {
        EntitySpec<? extends Application> spec = createAppSpec(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.parameters:",
                "  - simple");
        List<SpecParameter<?>> inputs = spec.getParameters();
        assertEquals(inputs.size(), 4);
        SpecParameter<?> firstInput = inputs.get(0);
        assertEquals(firstInput.getLabel(), "simple");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private AbstractBrooklynObjectSpec<?, ?> createSpec(CatalogItem<?, ?> item) {
        return (AbstractBrooklynObjectSpec<?,?>) catalog.createSpec((CatalogItem)item);
    }

    private EntitySpec<? extends Application> createAppSpec(String... lines) {
        return EntityManagementUtils.createEntitySpecForApplication(mgmt(), joinLines(lines));
    }

    private String inferItemType(Class<? extends BrooklynObject> testClass) {
        if (Entity.class.isAssignableFrom(testClass)) {
            return "entity";
        } else if (Policy.class.isAssignableFrom(testClass)) {
            return "policy";
        } else if (Location.class.isAssignableFrom(testClass)) {
            return "location";
        } else {
            throw new IllegalArgumentException("Class" + testClass + " not an entity, policy or location");
        }
    }
}
