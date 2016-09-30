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

package org.apache.brooklyn.test.framework;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.AttributeSensorAndConfigKey;

import com.google.common.base.Predicates;

/**
 * A test case that resolves target ID relative to an anchor entity.
 * <p>
 * For example, to run tests against a named child of an entity:
 * <pre>
 * services
 * - id: app
 *   brooklyn.children:
 *     id: child-id
 * - id: test-case
 *   brooklyn.config:
 *     anchor: $brooklyn:component("app").descendant("child-id")
 * </pre>
 * The anchor entity is resolved from the <code>anchor</code>, <code>target</code> or
 * <code>targetId</code>, in order of preference. The latter two are useful when using
 * the test with with entities like {@link LoopOverGroupMembersTestCase}. Values for
 * <code>target</code> will be overwritten with the resolved entity so child test
 * cases work as expected.
 */
@ImplementedBy(RelativeEntityTestCaseImpl.class)
public interface RelativeEntityTestCase extends TargetableTestComponent {

    AttributeSensorAndConfigKey<Entity, Entity> ANCHOR = ConfigKeys.newSensorAndConfigKey(Entity.class,
            "anchor",
            "Entity from which component should be resolved.");

    ConfigKey<DslComponent> COMPONENT = ConfigKeys.builder(DslComponent.class)
            .name("component")
            .description("The component to resolve against target")
            .constraint(Predicates.<DslComponent>notNull())
            .build();

}
