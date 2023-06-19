/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.camp.brooklyn.spi.dsl;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.resolve.jackson.WrappedValue;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.testng.annotations.Test;

public class DslPredicateYamlTest extends AbstractYamlTest {

    @Test
    public void testDslConfigSimple() throws Exception {
        final Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    "+TestEntity.CONF_STRING.getName()+": x",
                "    test.confPredicate:",
                "      config: "+TestEntity.CONF_STRING.getName(),
                "      equals: y");
        DslPredicates.DslPredicate predicate = app.config().get(TestEntity.CONF_PREDICATE);
        Asserts.assertFalse( predicate.apply(app) );
        app.config().set(TestEntity.CONF_STRING, "y");
        Asserts.assertTrue( predicate.apply(app) );
    }

    @Test
    public void testDslTags() throws Exception {
        final Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.tags:",
                "  - tag1",
                "  - color: blue");

        DslPredicates.DslPredicate predicate = TypeCoercions.coerce(MutableMap.of("tag", "tag1"), DslPredicates.DslPredicate.class);
        Asserts.assertTrue( predicate.apply(app) );

        predicate = TypeCoercions.coerce(MutableMap.of("tag", "tag2"), DslPredicates.DslPredicate.class);
        Asserts.assertFalse( predicate.apply(app) );

        predicate = TypeCoercions.coerce(MutableMap.of("tag", MutableMap.of("key", "color", "equals", "blue")), DslPredicates.DslPredicate.class);
        Asserts.assertTrue( predicate.apply(app) );

        predicate = TypeCoercions.coerce(MutableMap.of("tag", MutableMap.of("key", "color", "equals", "red")), DslPredicates.DslPredicate.class);
        Asserts.assertFalse( predicate.apply(app) );
    }

    @Test
    public void testDslConfigContainingDsl() throws Exception {
        final Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    "+TestEntity.CONF_STRING.getName()+": x",
                "    expected: x",
                "    test.confPredicate:",
                "      config: "+TestEntity.CONF_STRING.getName(),
                "      equals: $brooklyn:config(\"expected\")");
        DslPredicates.DslPredicate predicate = app.config().get(TestEntity.CONF_PREDICATE);
        Asserts.assertTrue( predicate.apply(app) );

        app.config().set(TestEntity.CONF_STRING, "y");
        Asserts.assertFalse( predicate.apply(app) );

        // nested DSL is resolved when predicate is _retrieved_, not when predicate is applied
        // this is simpler and more efficient, although it might be surprising
        app.config().set(ConfigKeys.newStringConfigKey("expected"), "y");
        Asserts.assertFalse( predicate.apply(app) );
        Asserts.assertEquals( ((DslPredicates.DslPredicateDefault)predicate).equals.get(), "x" );

        // per above, if we re-retrieve the predicate it should work fine
        predicate = app.config().get(TestEntity.CONF_PREDICATE);
        Asserts.assertTrue( predicate.apply(app) );
        Asserts.assertEquals( ((DslPredicates.DslPredicateDefault)predicate).equals.get(), "y" );
    }

    static class PredicateAndSpec {
        DslPredicates.DslPredicate test;
        EntitySpec<?> spec;
        EntitySpec<?> spec2;
    }

    static class PredicateAndSpecWrapped {
        DslPredicates.DslPredicate test;
        WrappedValue<EntitySpec<?>> spec;
        WrappedValue<EntitySpec<?>> spec2;
    }

    @Test
    public void testDslConfigWithWrappedValue() throws Exception {
        final Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    x: xv",
                "    test:",
                "      config: x",
                "      equals: $brooklyn:config(\"x\")",
                "    predicate_and_spec:",
                "      test: $brooklyn:config(\"test\")",
                "      spec:",
                "        type: " + BasicApplication.class.getName(),
                "        brooklyn.config:",
                "          xp: $brooklyn:config(\"x\")",
                "      spec2:",
                "       $brooklyn:entitySpec:",
                "        type: " + BasicApplication.class.getName(),
                "        brooklyn.config:",
                "          xp: $brooklyn:config(\"x\")",
                "");
        PredicateAndSpec ps = app.config().get(ConfigKeys.newConfigKey(PredicateAndSpec.class, "predicate_and_spec"));
        Asserts.assertTrue( ps.test.apply(app) );
        Asserts.assertEquals( ps.spec.getFlags().get("xp"), "xv" );  //comes in as flags
        Asserts.assertInstanceOf( ps.spec2.getConfig().get(ConfigKeys.newConfigKey(Object.class, "xp")), BrooklynDslDeferredSupplier.class);

        PredicateAndSpecWrapped psw = app.config().get(ConfigKeys.newConfigKey(PredicateAndSpecWrapped.class, "predicate_and_spec"));
        Asserts.assertTrue( psw.test.apply(app) );
        // TODO ideally putting in a wrapped value permits coercion but suppresses deep resolution
        // (but that is hard since deep resolution is baked in to config resolution prior to coercion)
        //Asserts.assertInstanceOf( psw.spec.get().getFlags().get("xp"), BrooklynDslDeferredSupplier.class);
        Asserts.assertEquals( psw.spec.get().getFlags().get("xp"), "xv" );

        Asserts.assertInstanceOf( psw.spec2.get().getConfig().get(ConfigKeys.newConfigKey(Object.class, "xp")), BrooklynDslDeferredSupplier.class);

        // TODO ideally this would be able to resolve the DSL expression for the test, but it doesn't
        // PredicateAndSpec psc = TypeCoercions.coerce(((AbstractEntity.BasicConfigurationSupport)app.config()).getRaw(ConfigKeys.newConfigKey(PredicateAndSpec.class, "predicate_and_spec")).get(), PredicateAndSpec.class);
    }

    @Test
    public void testDslPredicateConfigAsGuavaPredicate() throws Exception {
        DslPredicates.init();
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    "+TestEntity.CONF_STRING.getName()+": x",
                "    expected: x",
                "    test.confPredicate:",
                "          $brooklyn:object:\n" +
                "              type: com.google.common.base.Predicates\n" +
                "              factoryMethod.name: not\n" +
                "              factoryMethod.args:\n" +
                "                - $brooklyn:object:\n" +
                "                    type: com.google.common.base.Predicates\n" +
                "                    factoryMethod.name: equalTo\n" +
                "                    factoryMethod.args:\n" +
                "                      - $brooklyn:object:\n" +
                "                          type: org.apache.brooklyn.util.collections.MutableMap\n" +
                "                          factoryMethod.name: of");
        DslPredicates.DslPredicate predicate = app.config().get(TestEntity.CONF_PREDICATE);
        Asserts.assertFalse( predicate.apply(MutableMap.of()) );
        Asserts.assertTrue( predicate.apply(MutableMap.of("a","b")) );

        app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    "+TestEntity.CONF_STRING.getName()+": x",
                "    expected: x",
                "    test.confPredicate:",
                "      when: truthy");
        predicate = app.config().get(TestEntity.CONF_PREDICATE);
        Asserts.assertFalse( predicate.apply(MutableMap.of()) );
        Asserts.assertTrue( predicate.apply(MutableMap.of("a","b")) );
    }

    @Test
    public void testDslTargetLocationRetargets() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  location: localhost",
                "  brooklyn.config:",
                "    test.confPredicate:",
                "      target: location",
                "      tag: yes!");
        // 'location' can be expanded as list
        DslPredicates.DslPredicate predicate = app.config().get(TestEntity.CONF_PREDICATE);
        Asserts.assertFalse( predicate.apply(app) );
        app.getLocations().iterator().next().tags().addTag("yes!");
        Asserts.assertTrue( predicate.apply(app) );

        app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  location: localhost",
                "  brooklyn.config:",
                "    test.confPredicate:",
                "      target: locations",
                "      tag: yes!");
        // 'locations' requires has-element, cannot be auto-expanded
        predicate = app.config().get(TestEntity.CONF_PREDICATE);
        app.getLocations().iterator().next().tags().addTag("yes!");
        Asserts.assertFalse( predicate.apply(app) );

        app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  location: localhost",
                "  brooklyn.config:",
                "    test.confPredicate:",
                "      target: locations",
                "      has-element:",
                "        tag: yes!");
        // 'locations' works if has-element specified
        predicate = app.config().get(TestEntity.CONF_PREDICATE);
        Asserts.assertFalse( predicate.apply(app) );
        app.getLocations().iterator().next().tags().addTag("yes!");
        Asserts.assertTrue( predicate.apply(app) );

        app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  location: localhost",
                "  brooklyn.config:",
                "    test.confPredicate:",
                "      target: location",
                "      has-element:",
                "        tag: yes!");
        // 'location' also _accepts_ has-element (skips expanding)
        predicate = app.config().get(TestEntity.CONF_PREDICATE);
        Asserts.assertFalse( predicate.apply(app) );
        app.getLocations().iterator().next().tags().addTag("yes!");
        Asserts.assertTrue( predicate.apply(app) );
    }

    @Test
    public void testDslTargetLocationRetargetsWithoutGettingConfusedByConfig() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  location: localhost",
                "  brooklyn.config:",
                "    test.confPredicate:",
                "      target: location",
                "      config: locHasConf");
        // if 'location' is expanded as list, config is taken on the location
        Runnable resolveCheck = () -> {
            DslPredicates.DslPredicate predicate = app.config().get(TestEntity.CONF_PREDICATE);
            Asserts.assertFalse(predicate.apply(app));
            app.getLocations().iterator().next().config().set(ConfigKeys.newStringConfigKey("locHasConf"), "yes!");
            Asserts.assertTrue(predicate.apply(app));
        };
        // works in a task
        DynamicTasks.submit(Tasks.create("check config", resolveCheck), app).get();

        // outside of a task we get a nice error
        Asserts.assertFailsWith(resolveCheck, e -> Asserts.expectedFailureContainsIgnoreCase(e, "locHasConf", "Localhost", "resolve", "entity task"));
    }

    @Test
    public void testDslTargetTagRetargets() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    test.confPredicate:",
                "      target: tag",
                "      equals: yes!");
        // 'tag' can be expanded as list
        DslPredicates.DslPredicate predicate = app.config().get(TestEntity.CONF_PREDICATE);
//        Asserts.assertFalse( predicate.apply(app) );
        app.tags().addTag("yes!");
        Asserts.assertTrue( predicate.apply(app) );

        app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    test.confPredicate:",
                "      target: tags",
                "      equals: yes!");
        // 'tags' requires has-element, cannot be auto-expanded
        predicate = app.config().get(TestEntity.CONF_PREDICATE);
        app.tags().addTag("yes!");
        Asserts.assertFalse( predicate.apply(app) );

        app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    test.confPredicate:",
                "      target: tags",
                "      has-element:",
                "        equals: yes!");
        // 'tags' works if has-element specified
        predicate = app.config().get(TestEntity.CONF_PREDICATE);
        Asserts.assertFalse( predicate.apply(app) );
        app.tags().addTag("yes!");
        Asserts.assertTrue( predicate.apply(app) );

        app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    test.confPredicate:",
                "      target: tag",
                "      has-element:",
                "        equals: yes!");
        // 'tag' also _accepts_ has-element (skips expanding)
        predicate = app.config().get(TestEntity.CONF_PREDICATE);
        Asserts.assertFalse( predicate.apply(app) );
        app.tags().addTag("yes!");
        Asserts.assertTrue( predicate.apply(app) );
    }

    @Test
    public void testDslTargetChildRetargets() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    test.confPredicate:",
                "      target: child",
                "      tag: yes!");
        // 'child' can be expanded as list
        DslPredicates.DslPredicate predicate = app.config().get(TestEntity.CONF_PREDICATE);
        Asserts.assertFalse( predicate.apply(app) );
        app.addChild(EntitySpec.create(BasicEntity.class).tag("yes!"));
        Asserts.assertTrue( predicate.apply(app) );

        app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    test.confPredicate:",
                "      target: children",
                "      tag: yes!");
        // 'children' requires has-element, cannot be auto-expanded
        predicate = app.config().get(TestEntity.CONF_PREDICATE);
        app.addChild(EntitySpec.create(BasicEntity.class).tag("yes!"));
        Asserts.assertFalse( predicate.apply(app) );

        app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    test.confPredicate:",
                "      target: children",
                "      has-element:",
                "        tag: yes!");
        // 'children' works if has-element specified
        predicate = app.config().get(TestEntity.CONF_PREDICATE);
        Asserts.assertFalse( predicate.apply(app) );
        app.addChild(EntitySpec.create(BasicEntity.class).tag("yes!"));
        Asserts.assertTrue( predicate.apply(app) );

        app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    test.confPredicate:",
                "      target: child",
                "      has-element:",
                "        tag: yes!");
        // 'child' also _accepts_ has-element (skips expanding)
        predicate = app.config().get(TestEntity.CONF_PREDICATE);
        Asserts.assertFalse( predicate.apply(app) );
        app.addChild(EntitySpec.create(BasicEntity.class).tag("yes!"));
        Asserts.assertTrue( predicate.apply(app) );
    }

}
