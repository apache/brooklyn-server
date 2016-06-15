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

import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.test.entity.TestEntityImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class TestEntityWithEffectorsImpl extends TestEntityImpl implements TestEntityWithEffectors {
    private static final Logger log = LoggerFactory.getLogger(TestEntityWithEffectorsImpl.class);

    @Override
    public void resetPassword(String newPassword, Integer secretPin) {
        log.info("Invoked effector from resetPassword with params {} {}", new Object[] {newPassword, secretPin});
        assert newPassword != null;
    }

    @Override
    public void resetPasswordThrowsException(String newPassword, Integer secretPin) {
        log.info("Invoked effector from resetPasswordThrowsException with params {} {}", new Object[] {newPassword, secretPin});
        throw new RuntimeException("Simulate failure in resetPasswordThrowsException");
    }
    
    @Override
    public void invokeUserAndPassword(String user,String newPassword, String paramDefault) {
        log.info("Invoked effector from invokeUserAndPassword with params {} {} {}", new Object[] {user, newPassword, paramDefault});
        assert user != null;
        assert newPassword != null;
    }

    @Override
    public void resetPasswordOnChildren(String newPassword, Integer secretPin) throws Exception {
        for (TestEntityWithEffectors child : Iterables.filter(getChildren(), TestEntityWithEffectors.class)) {
            Entities.invokeEffector(this, child, RESET_PASSWORD, ImmutableMap.of("newPassword", newPassword, "secretPin", secretPin)).get();
            child.resetPassword(newPassword, secretPin);
        }
    }
}
