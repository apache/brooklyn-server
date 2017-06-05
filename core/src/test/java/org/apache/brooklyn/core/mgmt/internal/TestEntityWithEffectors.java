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
import org.apache.brooklyn.core.effector.MethodEffector;
import org.apache.brooklyn.core.test.entity.TestEntity;

/**
 * Entity for testing that secret effector parameters are:
 * <ul>
 *   <li>excluded from the activities view
 *   <li>not logged
 *   <li>masked out in the UI
 * </ul>
 * Of those, only the first is unit-tested.
 * 
 * To test manually...
 * 
 * Configure logback to log everything at trace:
 * <pre>
 * {@code
 * <configuration>
 *     <include resource="logback-main.xml"/>
 *     <logger name="org.apache.brooklyn" level="TRACE"/>
 *     <logger name="brooklyn" level="TRACE"/>
 * </configuration>
 * }
 * </pre>
 *
 * Ensure that {@code TestEntityWithEffectors} (and its super-class {@TestEntity}) is on the
 * classpath, and then run Brooklyn with the above log configuration file:
 * <pre>
 * {@code
 * cp /path/to/test-entities.jar ./lib/dropins/
 * export JAVA_OPTS="-Xms256m -Xmx1g -Dlogback.configurationFile=/path/to/logback-trace.xml"
 * ./bin/brooklyn launch --persist auto --persistenceDir /path/to/persistedState
 * }
 * </pre>
 *
 * Deploy the blueprint below:
 * <pre>
 * {@code
 * services:
 * - type: org.apache.brooklyn.core.mgmt.internal.TestEntityWithEffectors
 *   brooklyn.children:
 *   - type: org.apache.brooklyn.core.mgmt.internal.TestEntityWithEffectors
 * }
 * </pre>
 * 
 * Invoke the effector (e.g. {@code resetPasswordOnChildren("mypassword", 12345678)} on the 
 * top-level {@code TestEntityWithEffectors}, and the effector 
 * {@code resetPasswordThrowsException("mypassword", 12345678)}).
 * 
 * Then manually check:
 * <ul>
 *   <li>the parameter values were masked out while being entered
 *   <li>activity view of each TestEntityWithEffectors, that it does not show those parameter values
 *   <li>{@code grep -E "mypassword|12345678" *.log}
 *   <li>{@code pushd /path/to/persistedState && grep -r -E "mypassword|12345678" *}
 * </ul>
 */
@ImplementedBy(TestEntityWithEffectorsImpl.class)
public interface TestEntityWithEffectors extends TestEntity {
    
    org.apache.brooklyn.api.effector.Effector<Void> RESET_PASSWORD = new MethodEffector<Void>(TestEntityWithEffectors.class, "resetPassword");
    
    @Effector(description = "Reset password")
    void resetPassword(@EffectorParam(name = "newPassword") String newPassword, @EffectorParam(name = "secretPin") Integer secretPin);

    @Effector(description = "Reset password throws exception")
    void resetPasswordThrowsException(@EffectorParam(name = "newPassword") String newPassword, @EffectorParam(name = "secretPin") Integer secretPin);

    @Effector(description = "Reset User and password effector")
    void invokeUserAndPassword(@EffectorParam(name = "user") String user,
                               @EffectorParam(name = "newPassword", defaultValue = "Test") String newPassword,
                               @EffectorParam(name = "paramDefault", defaultValue = "DefaultValue") String paramDefault);
    
    @Effector(description = "Reset password on children")
    void resetPasswordOnChildren(@EffectorParam(name = "newPassword") String newPassword, @EffectorParam(name = "secretPin") Integer secretPin) throws Exception;
}
