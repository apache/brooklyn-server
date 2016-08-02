/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.test.framework;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;

import com.google.common.reflect.TypeToken;

/**
 * Entity that logically groups other test entities
 */
@ImplementedBy(value = TestCaseImpl.class)
public interface TestCase extends TargetableTestComponent {

    @SuppressWarnings("serial")
    ConfigKey<EntitySpec<?>> ON_ERROR_SPEC = ConfigKeys.builder(new TypeToken<EntitySpec<?>>() {})
            .name("on.error.spec")
            .description("Spec of entity to instantiate (and start, if startable) if the test-case fails")
            .parentInheritance(ConfigInheritance.NONE)
            .typeInheritance(ConfigInheritance.ALWAYS)
            .build();
}
