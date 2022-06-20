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
package org.apache.brooklyn.util.core.predicates;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.yaml.Yamls;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.function.Consumer;

public class DslPredicateEntityTest extends BrooklynAppUnitTestSupport {

    static {
        DslPredicates.init();
    }

    @Test
    public void testConfigEquals() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "config", TestEntity.CONF_OBJECT.getName(),
                "equals", "x"), DslPredicates.DslPredicate.class);

        app.config().set(TestEntity.CONF_OBJECT, "x");
        Asserts.assertTrue(p.test(app));

        app.config().set(TestEntity.CONF_OBJECT, "y");
        Asserts.assertFalse(p.test(app));
    }

    // TODO would be nice to add tag, location tests -- but manual checking has confirmed they work

    @Test
    // would be nice to support all the EntityPredicates with the new mechanism; but coercion works well enough in practice,
    // and doing it at deserialization time is difficult
    public void testNonDslConfigEqualsPredicateDeserializesInCertainSpecificCases() throws JsonProcessingException {
        // note the pre-existing EntityPredicates that use Guava don't work via this mechanism
        Predicate<Entity> nonDslPredicate = EntityPredicates.configCustomEqualTo(TestEntity.CONF_OBJECT.getName(), "x");
        ObjectMapper mapper = BeanWithTypeUtils
                .newMapper
                // and YAML known not to work because the type of the ALWAYS_TRUE guava predicate used in the config key is not written, and the string cannot be deserialized without that
//                .newYamlMapper
                (mgmt, true, null, true);
        String mapS = mapper.writerFor(Object.class).writeValueAsString(nonDslPredicate);
        java.util.function.Predicate p = (java.util.function.Predicate) mapper.readValue(mapS, Object.class);


        Consumer<java.util.function.Predicate> test = pi -> {
            app.config().set(TestEntity.CONF_OBJECT, "x");
            Asserts.assertTrue(pi.test(app));
            app.config().set(TestEntity.CONF_OBJECT, "y");
            Asserts.assertFalse(pi.test(app));
        };
        test.accept(p);

        Map mapM = (Map) Yamls.parseAll(mapS).iterator().next();
        // this also doesn't work because we cannot read a ConfigKeySatisfies predicate when expecting a DslPredicate
        //DslPredicates.DslPredicate p2 = TypeCoercions.coerce(mapM, DslPredicates.DslPredicate.class);
        // but this two-stage process does
        Predicate p2a = TypeCoercions.coerce(mapM, Predicate.class);
        p = TypeCoercions.coerce(p2a, DslPredicates.DslPredicate.class);

        test.accept(p);
    }

    @Test(groups="WIP")  // does not work; guava predicates are not easily made serializable via jackson; better not to use them.
    // (they do not have 0-arg constructors, the 1-arg constructors are private _and_ the parameter names are not stored.)
    public void testGuavaPredicate() throws JsonProcessingException {
        String s = "{\"type\":\"com.google.common.base.Predicates$IsEqualToPredicate\",\"target\":\"x\"}";
        ObjectMapper mapper = BeanWithTypeUtils.newYamlMapper(mgmt, true, null, true);
        Predicate p = mapper.readValue(s, Predicate.class);
        Asserts.assertTrue(p.test("x"));
        Asserts.assertFalse(p.test("y"));
    }


}
