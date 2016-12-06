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
package org.apache.brooklyn.core.mgmt.rebind;

import java.util.Collection;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestApplicationNoEnrichersImpl;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

public class RebindTestFixtureWithApp extends RebindTestFixture<TestApplication> {

    /** set null not to set, or a boolean to set explicitly */
    protected Boolean shouldSkipOnBoxBaseDirResolution() {
        // TODO Change default to true; starting with this as null for backwards compatibility!
        return null;
    }

    protected TestApplication createApp() {
        EntitySpec<TestApplication> spec = EntitySpec.create(TestApplication.class, TestApplicationNoEnrichersImpl.class);
        if (shouldSkipOnBoxBaseDirResolution()!=null) {
            spec.configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, shouldSkipOnBoxBaseDirResolution());
        }
        return origManagementContext.getEntityManager().createEntity(spec);
    }
    
    @Override
    protected TestApplication rebind(RebindOptions options) throws Exception {
        if (options.applicationChooserOnRebind == null) {
            // Some sub-classes will have added additional apps before rebind. We must return an
            // app of type "TestApplication"; otherwise we'll get a class-cast exception.
            options = RebindOptions.create(options);
            options.applicationChooserOnRebind(new Function<Collection<Application>, Application>() {
                @Override public Application apply(Collection<Application> input) {
                    return Iterables.find(input, Predicates.instanceOf(TestApplication.class));
                }});
        }
        return super.rebind(options);
    }
}
