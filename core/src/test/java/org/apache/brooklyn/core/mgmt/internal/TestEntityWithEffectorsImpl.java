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

import org.apache.brooklyn.core.test.entity.TestEntityImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestEntityWithEffectorsImpl extends TestEntityImpl implements TestEntityWithEffectors {
    private static final Logger log = LoggerFactory.getLogger(TestEntityWithEffectorsImpl.class);

    @Override
    public Void resetPassword(String newPassword, Integer secretPin) {
        log.info("Invoked effector from resetPassword with params {} {}", new Object[] {newPassword, secretPin});
        assert newPassword != null;
        return null;
    }

    @Override
    public Void invokeUserAndPassword(String user,String newPassword, String paramDefault) {
        log.info("Invoked effector from invokeUserAndPassword with params {} {} {}", new Object[] {user, newPassword, paramDefault});
        assert user != null;
        assert newPassword != null;
        return null;
    }
}
