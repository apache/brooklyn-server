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
package org.apache.brooklyn.core.policy;

import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.core.entity.EntityAdjuncts;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;

/** @deprecated since 0.13.0 use {@link EntityAdjuncts} */
@Deprecated
public class Policies {

    /** @deprecated since 0.13.0, use {@link EntityAdjuncts#inferAdjunctStatus(org.apache.brooklyn.api.objs.EntityAdjunct)} */
    @Deprecated
    public static Lifecycle getPolicyStatus(Policy p) {
        return EntityAdjuncts.inferAdjunctStatus(p);
    }
    
}
