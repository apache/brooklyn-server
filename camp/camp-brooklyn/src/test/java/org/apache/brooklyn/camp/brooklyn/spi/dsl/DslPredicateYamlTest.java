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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslTestObjects.DslTestCallable;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslTestObjects.DslTestSupplierWrapper;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslTestObjects.TestDslSupplier;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslTestObjects.TestDslSupplierValue;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.custom.UserSuppliedPackageType;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.entity.stock.BasicStartable;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;

import static org.testng.Assert.assertEquals;

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

        // per above, if we re-retrieve the predicate it should work fine
        predicate = app.config().get(TestEntity.CONF_PREDICATE);
        Asserts.assertTrue( predicate.apply(app) );
    }

}
