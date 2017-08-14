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

package org.apache.brooklyn.entity.group;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.objs.SpecParameter;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.mgmt.rebind.RebindOptions;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixture;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicApplicationImpl;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Collection;
import java.util.List;

import static org.apache.brooklyn.test.Asserts.assertTrue;

public class EntitySpecRebindTest extends RebindTestFixture<BasicApplication> {

    @Test
    public void testThrottleAppliesAfterRebind() throws Exception {
        addMemento(BrooklynObjectType.ENTITY, "entity-spec-value", "sr6qljzg0j");
        RebindOptions rebindOptions = RebindOptions.create();
        rebindOptions.applicationChooserOnRebind((Function<Collection<Application>, Application>) input ->
                Iterables.find(input, o -> o instanceof BasicApplication && o.getDisplayName().equals("BROOKLYN-519 backwards compatibility")));
        Entity t = rebind(rebindOptions);
        List<SpecParameter<?>> specParameters = t.getConfig(
                ConfigKeys.newConfigKey(
                        new TypeToken<EntitySpec<?>>() {},
                        "childSpec"))
                .getParameters();
        assertTrue(specParameters.size() > 10);

        SpecParameter<?> dynamicClusterParameter = Iterables.find(specParameters, specParameter -> specParameter.getLabel().equals("dynamiccluster.memberspec"));
        assertTrue(EntitySpec.class.equals(dynamicClusterParameter.getConfigKey().getTypeToken().getRawType()), "Should have deserialized config key type.");
    }

    @Override
    protected BasicApplication createApp() {
        EntitySpec<BasicApplication> spec = EntitySpec.create(BasicApplication.class, BasicApplicationImpl.class);
            spec.configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true);
        return origManagementContext.getEntityManager().createEntity(spec);
    }

    protected void addMemento(BrooklynObjectType type, String label, String id) throws Exception {
        String mementoFilename = label+"-"+id;
        String memento = Streams.readFullyString(getClass().getResourceAsStream(mementoFilename));

        File persistedFile = getPersistanceFile(type, id);
        Files.write(memento.getBytes(), persistedFile);
    }

    protected File getPersistanceFile(BrooklynObjectType type, String id) {
        String dir;
        switch (type) {
            case ENTITY: dir = "entities"; break;
            default: throw new UnsupportedOperationException("type="+type);
        }
        return new File(mementoDir, Os.mergePaths(dir, id));
    }
}
