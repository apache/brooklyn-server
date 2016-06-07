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
package org.apache.brooklyn.core.mgmt.internal;

import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.core.annotation.Effector;
import org.apache.brooklyn.core.annotation.EffectorParam;
import org.apache.brooklyn.core.test.entity.TestEntity;

@ImplementedBy(TestEntityWithEffectorsImpl.class)
public interface TestEntityWithEffectors extends TestEntity {
    @Effector(description = "Reset password")
    Void resetPassword(@EffectorParam(name = "newPassword") String param1, @EffectorParam(name = "secretPin") Integer secretPin);

    @Effector(description = "Reset User and password effector")
    Void invokeUserAndPassword(@EffectorParam(name = "user") String user,
                               @EffectorParam(name = "newPassword", defaultValue = "Test") String newPassword,
                               @EffectorParam(name = "paramDefault", defaultValue = "DefaultValue") String paramDefault);
}
