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

import static org.apache.brooklyn.test.Asserts.assertEquals;

import java.util.Iterator;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class ProxyEffectorYamlTest extends AbstractYamlTest {

    @Test
    public void testProxyEffector() throws Exception {
        final String effectorName = "proxy-effector";
        Entity app = createAndStartApplication(
                "location: localhost",
                "services:",
                "- id: target",
                "  type: " + TestEntity.class.getName(),
                "- type: " + BasicEntity.class.getName(),
                "  brooklyn.initializers:",
                "  - type: org.apache.brooklyn.core.effector.ProxyEffector",
                "    brooklyn.config:",
                "      name: " + effectorName,
                "      targetEntity: $brooklyn:entity(\"target\")",
                "      targetEffector: identityEffector"
        );
        waitForApplicationTasks(app);

        assertEquals(app.getChildren().size(), 2, "expected two elements in " + app.getChildren());
        Entity basicEntity;
        Iterator<Entity> ei = app.getChildren().iterator();
        basicEntity = ei.next();
        if (!BasicEntity.class.isAssignableFrom(basicEntity.getClass())) {
            basicEntity = ei.next();
        }

        Effector<?> effector = basicEntity.getEntityType().getEffectorByName(effectorName).get();
        Object result = basicEntity.invoke(effector, ImmutableMap.of("arg", "hello, world")).get();
        assertEquals(((String) result).trim(), "hello, world");
}

    @Test
    public void testThrowsIfTargetDoesNotResolve() throws Exception {
        final String effectorName = "proxy-effector";
        Entity app = createAndStartApplication(
                "location: localhost",
                "services:",
                "- type: " + BasicEntity.class.getName(),
                "  brooklyn.initializers:",
                "  - type: org.apache.brooklyn.core.effector.ProxyEffector",
                "    brooklyn.config:",
                "      name: " + effectorName,
                "      targetEntity: $brooklyn:entity(\"skhbfskdbf\")",
                "      targetEffector: identityEffector"
        );
        waitForApplicationTasks(app);
        Entity basicEntity = app.getChildren().iterator().next();
        Effector<?> effector = basicEntity.getEntityType().getEffectorByName(effectorName).get();
        try {
            basicEntity.invoke(effector, ImmutableMap.<String, Object>of()).get();
            Asserts.shouldHaveFailedPreviously("expected exception when invoking effector that does not exist");
        } catch (Exception e) {
            Asserts.expectedFailure(e);
        }
    }

}
