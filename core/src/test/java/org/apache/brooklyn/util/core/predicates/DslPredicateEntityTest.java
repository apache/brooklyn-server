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
import org.testng.annotations.Test;

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

    // TODO pre-existing EntityPredicates don't work via this mechanism
    // - doesn't deserialize with bean serializer (won't find constructor)
    // - don't coerce automatically
    // - maybe add'l problems with yaml?
    @Test(groups="WIP")
    public void testNonDslConfigEqualsPredicateDeserialize() throws JsonProcessingException {
        Predicate<Entity> nonDslPredicate = EntityPredicates.configEqualTo(TestEntity.CONF_OBJECT.getName(), "x");
        ObjectMapper mapper = BeanWithTypeUtils
                .newMapper
                // TODO yaml should also work
//                .newYamlMapper
                (mgmt, true, null, true);
        String mapS = mapper.writerFor(Object.class).writeValueAsString(nonDslPredicate);
        java.util.function.Predicate p = (java.util.function.Predicate) mapper.readValue(mapS, Object.class);

        // TODO ideally this would also work
//        Map mapM = (Map) Yamls.parseAll(mapS).iterator().next();
//        DslPredicates.DslPredicate p = TypeCoercions.coerce(mapM, DslPredicates.DslPredicate.class);

        app.config().set(TestEntity.CONF_OBJECT, "x");
        Asserts.assertTrue(p.test(app));

        app.config().set(TestEntity.CONF_OBJECT, "y");
        Asserts.assertFalse(p.test(app));
    }

}
